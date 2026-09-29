from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.filter(F.month("end_date").isin([4, 6, 11])).alias("subscriptions"), F.col("customers.id") == F.col("subscriptions.customer_id"), "left_semi")
    .select(
        F.col("customers.id"),
        F.col("customers.age"),
        F.col("customers.segment"),
        F.col("customers.is_active"),
    )
)
