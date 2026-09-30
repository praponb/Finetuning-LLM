from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.segment") != "SMB")
    .groupBy(F.col("c.city"))
    .agg(
        F.sum(F.when(F.col("o.status") == "CANCELLED", F.col("o.discount")).otherwise(0)).alias("cancelled_discount"),
    )
)
