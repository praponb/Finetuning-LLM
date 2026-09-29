from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val_filled").desc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy("val_filled_cume_dist")
w4 = Window.orderBy(F.col("val").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("val"))
)

base = (
    lines
    .filter(F.col("val").between(48, 57))
    .select(
        "*",
        F.coalesce("val", F.lit(0)).alias("val_filled"),
        F.substring("plan", 1, 4).alias("plan_prefix"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.cume_dist().over(w).alias("val_filled_cume_dist"),
        F.min("val_filled").over(w2).alias("plan_min_val_filled"),
    )
)

enriched_price_tiers = (
    enriched
    .withColumn("val_filled_cume_dist_percent_rank", F.percent_rank().over(w3))
    .withColumn("val_dense_rank", F.dense_rank().over(w4))
)

grouped = (
    enriched_price_tiers
    .groupBy(F.date_trunc("quarter", "effective_from").alias("quarter_start"))
    .agg(
        F.percentile_approx("plan_min_val_filled", [0.25, 0.5, 0.75]).alias("plan_min_val_filled_quartiles"),
        F.count("*").alias("event_count"),
    )
)

result = (
    grouped
    .select("quarter_start")
    .orderBy(F.col("quarter_start").asc_nulls_last())
)
