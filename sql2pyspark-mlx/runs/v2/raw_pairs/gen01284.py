from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.length("email") > 8).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("s.start_date"),
        F.col("s.end_date"),
    )
    .orderBy("id")
    .limit(5)
)
