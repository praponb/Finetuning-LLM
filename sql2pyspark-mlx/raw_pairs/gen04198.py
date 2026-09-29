from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "created_at")).orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("customer_id")
w_ordered = Window.partitionBy("customer_id").orderBy(F.col("product_id").desc())
w4 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.orderBy(F.col("product_id").asc_nulls_first())
w6 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w7 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w8 = Window.orderBy(F.col("product_id").asc_nulls_last())
w9 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

exploded = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.promo"),
    )
)

with_windows = (
    exploded
    .withColumn("qty_count_1h", F.count("qty").over(w))
    .withColumn("customer_id_count_qty", F.count("qty").over(w2))
)

remaining_keys = (
    with_windows
    .select("id")
    .intersect(
        df_carts
        .filter(F.to_date("created_at") < "2023-05-01")
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows_carts = (
    verified
    .filter(~F.col("promo").isin(["VIP", "SPRING10", "BUNDLE"]))
    .select(
        "id",
        "created_at",
        "product_id",
        "customer_id_count_qty",
        F.percent_rank().over(w_ordered).alias("product_id_percent_rank"),
        F.max("qty").over(w4).alias("qty_max_2h"),
    )
)

cleaned = (
    with_windows_carts
    .filter(F.to_date("created_at") < "2024-10-05")
    .select(
        "created_at",
        "product_id",
        "product_id_percent_rank",
        F.round("product_id", 0).alias("product_id_rounded"),
        F.round((F.col("customer_id_count_qty") * 100), 2).alias("customer_id_count_qty_scaled"),
    )
)

valid = (
    cleaned
    .filter(F.col("created_at") >= F.lit("2025-03-02").cast("date"))
    .select(
        "*",
        (F.col("customer_id_count_qty_scaled") + F.col("product_id")).alias("customer_id_count_qty_scaled_plus_product_id"),
    )
)

metrics = (
    valid
    .select(
        "created_at",
        "product_id",
        F.dense_rank().over(w5).alias("product_id_dense_rank"),
        F.count("customer_id_count_qty_scaled").over(w6).alias("customer_id_count_qty_scaled_count_2h"),
        F.sum("product_id_percent_rank").over(w7).alias("product_id_percent_rank_sum_15m"),
        F.cume_dist().over(w8).alias("product_id_cume_dist"),
        F.avg("product_id").over(w9).alias("product_id_avg_1h"),
    )
)

stats = (
    metrics
    .groupBy(
        F.when(F.col("product_id_cume_dist") >= 0.67, "LARGE").otherwise("SMALL").alias("product_id_cume_dist_tier"),
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
    )
    .agg(
        F.max("created_at").alias("last_created_at"),
        F.max("created_at").alias("last_seen"),
        F.round(F.avg("product_id"), 2).alias("avg_product_id"),
        F.percentile_approx("product_id_dense_rank", 0.5).alias("product_id_dense_rank_p50"),
        F.percentile_approx("customer_id_count_qty_scaled_count_2h", [0.25, 0.5, 0.75]).alias("customer_id_count_qty_scaled_count_2h_quartiles"),
    )
)

result = (
    stats
    .select(
        "product_id_cume_dist_tier",
        "hour_window",
        "last_created_at",
        "last_seen",
        "product_id_dense_rank_p50",
    )
    .orderBy(F.col("hour_window").desc())
)
