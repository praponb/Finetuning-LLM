from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    exploded
    .groupBy(
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
    )
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.avg("element").alias("avg_element"),
    )
    .orderBy(F.col("avg_element").asc_nulls_last())
)
