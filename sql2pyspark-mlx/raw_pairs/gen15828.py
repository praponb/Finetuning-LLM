from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.city").alias("city"), F.col("e.plan").alias("plan"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.monthly_fee")).alias("max_monthly_fee"),
        F.grouping_id().alias("gid"),
    )
)
