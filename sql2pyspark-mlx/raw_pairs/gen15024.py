from pyspark.sql import functions as F

rollup_totals = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .rollup(F.col("c.country").alias("country"), F.col("a.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.monthly_fee")).alias("max_monthly_fee"),
        F.grouping_id().alias("gid"),
    )
)

result = rollup_totals.select("status", "max_monthly_fee", "gid")
