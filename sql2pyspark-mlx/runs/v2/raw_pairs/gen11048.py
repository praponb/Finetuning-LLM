from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.col("c.country"),
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
    )
    .agg(
        F.min(F.col("s.end_date")).alias("min_end_date"),
        F.count("*").alias("row_count"),
    )
)
