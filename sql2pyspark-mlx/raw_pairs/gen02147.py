from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "created_at"))
w_grp = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select("id", "created_at", "coupon_codes", F.col("item.product_id"))
)

cleaned = (
    flattened
    .filter(F.col("created_at") < "2023-05-29")
    .select("*", F.ceil("product_id").alias("product_id_whole"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "product_id_whole",
        F.count("product_id_whole").over(w).alias("created_at_period_count_product_id_whole"),
        F.sum("product_id").over(w_grp).alias("product_id_sum_30m"),
    )
)

cleaned_carts = (
    with_windows
    .filter(F.col("created_at_period_count_product_id_whole") != 7)
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "product_id_whole",
        "created_at_period_count_product_id_whole",
        "product_id_sum_30m",
        (F.col("product_id_whole") + F.col("product_id_sum_30m")).alias("product_id_whole_plus_product_id_sum_30m"),
    )
)

matched_keys = (
    cleaned_carts
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "WELCOME5"))
        .select("id")
    )
)

reconciled = (
    cleaned_carts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("product_id_whole_plus_product_id_sum_30m") >= 84, "HIGH").otherwise("LOW").alias("product_id_whole_plus_product_id_sum_30m_tier"),
        F.when(F.col("product_id_sum_30m") >= 48, "PREMIUM").otherwise("STANDARD").alias("product_id_sum_30m_tier"),
    )
    .agg(
        F.avg("created_at_period_count_product_id_whole").alias("avg_created_at_period_count_product_id_whole"),
        F.percentile_approx("product_id_sum_30m", [0.25, 0.5, 0.75]).alias("product_id_sum_30m_quartiles"),
    )
)
