from pyspark.sql import functions as F

by_dimension = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .groupingSets(
        [["segment", "country", "plan"], ["segment"]],
        F.col("d.segment").alias("segment"),
        F.col("d.country").alias("country"),
        F.col("src.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.monthly_fee")).alias("min_monthly_fee"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    by_dimension
    .select("segment", "country", "plan", "rows_total", "min_monthly_fee")
    .orderBy(F.col("country").desc())
)
