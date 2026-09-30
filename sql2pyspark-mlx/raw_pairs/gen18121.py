from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("channel")
    .agg(
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.avg("discount_pct").alias("avg_discount_pct"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .filter(F.col("avg_discount_pct") >= 0)
    .orderBy(F.col("avg_discount_pct").asc_nulls_last())
)
