from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "inner")
    .cube(F.col("dim.city").alias("city"), F.col("e.plan").alias("plan"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("e.monthly_fee")).alias("avg_monthly_fee"),
    )
)
