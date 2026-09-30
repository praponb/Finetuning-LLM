from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy(F.date_trunc("week", "created_at"), "promo")
w2 = Window.partitionBy(F.year("created_at_day"), "promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_recent = Window.orderBy(F.col("created_at_period_min_product_id_rounded").desc())

lines = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select("id", "created_at", F.col("item.product_id"), F.col("item.promo"))
)

cleaned = (
    lines
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (F.to_date("created_at") < "2025-01-05"))
    .withColumn("product_id_rounded", F.round("product_id", 2))
)

cleaned_carts = (
    cleaned
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 17:00:00"))) & F.col("promo").isNotNull())
    .select(
        "id",
        "created_at",
        "product_id",
        "promo",
        "product_id_rounded",
        F.col("created_at").cast("date").alias("created_at_day"),
    )
)

filtered = (
    cleaned_carts
    .filter(F.col("promo").isNotNull())
    .select(
        "id",
        "created_at",
        "product_id",
        "promo",
        "product_id_rounded",
        "created_at_day",
        F.add_months("created_at_day", 12).alias("created_at_day_plus_months"),
        F.initcap(F.trim("promo")).alias("promo_title"),
        F.coalesce("promo", F.lit("NONE")).alias("promo_clean"),
    )
)

with_windows = (
    filtered
    .filter(F.col("product_id").between(4, 6))
    .select(
        "*",
        F.min("product_id_rounded").over(w_grp).alias("created_at_period_min_product_id_rounded"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.max("created_at_period_min_product_id_rounded").over(w2).alias("created_at_period_min_product_id_rounded_max_2h"),
        F.avg("product_id_rounded").over(w3).alias("product_id_rounded_avg_1h"),
        F.max("product_id_rounded").over(w4).alias("product_id_rounded_max_30m"),
        F.dense_rank().over(w_recent).alias("created_at_period_min_product_id_rounded_dense_rank"),
    )
)

filtered_carts = (
    scored
    .filter(F.col("promo").isNotNull() & F.month("created_at_day").isin([4, 5, 12]))
    .withColumn("created_at_period_min_product_id_rounded_dense_rank_level", F.when(F.col("created_at_period_min_product_id_rounded_dense_rank") >= 8, "High").when(F.col("created_at_period_min_product_id_rounded_dense_rank") >= 3, "Medium").otherwise("Low"))
)

result = (
    filtered_carts
    .select(
        "product_id",
        "promo_title",
        "created_at_period_min_product_id_rounded_max_2h",
        "product_id_rounded_avg_1h",
        "product_id_rounded_max_30m",
        "created_at_period_min_product_id_rounded_dense_rank",
        "created_at_period_min_product_id_rounded_dense_rank_level",
    )
    .orderBy("created_at_period_min_product_id_rounded_dense_rank")
)
