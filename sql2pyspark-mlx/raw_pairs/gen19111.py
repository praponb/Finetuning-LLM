from pyspark.sql import functions as F

dim_totals = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .cube(F.col("d.country").alias("country"), F.col("src.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("src.monthly_fee")), 2).alias("total_monthly_fee"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    dim_totals
    .select("num_rows", "total_monthly_fee")
    .orderBy(F.col("total_monthly_fee").desc(), F.col("num_rows").desc())
)
