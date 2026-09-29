from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("YEAR", "effective_from").alias("year_start"))
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.percentile_approx("element", 0.95).alias("element_p95"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("distinct_plans").desc_nulls_last())
)
