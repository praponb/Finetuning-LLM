from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("value").orderBy(F.col("created_at_month").desc())
w3 = Window.partitionBy("value").orderBy(F.col("created_at_month").desc_nulls_first())

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

flattened = (
    exploded
    .select("id", "created_at", "v", F.explode_outer("coupon_codes").alias("value"))
)

filtered = (
    flattened
    .filter(F.col("value").isin(["WELCOME5", "BUNDLE", "VIP"]))
    .select(
        "*",
        F.month("created_at").alias("created_at_month"),
        F.coalesce("value", F.lit("NONE")).alias("value_filled"),
    )
)

cleaned = (
    filtered
    .filter((F.col("created_at_month") > 27) & (F.col("created_at") > F.lit("2025-03-01").cast("date")))
    .select(
        "*",
        F.coalesce("value", F.lit("N/A")).alias("value_filled_2"),
        F.coalesce("value", F.lit("UNKNOWN")).alias("resolved_value"),
    )
)

enriched = (
    cleaned
    .select("*", F.avg("created_at_month").over(w).alias("created_at_month_avg_1h"))
)

metrics = (
    enriched
    .filter(F.col("created_at_month") >= 38)
    .select(
        "id",
        "v",
        "created_at_month_avg_1h",
        F.rank().over(w2).alias("created_at_month_rank"),
        F.rank().over(w3).alias("created_at_month_rank_2"),
    )
)

result = metrics.select("v", "created_at_month_rank")
