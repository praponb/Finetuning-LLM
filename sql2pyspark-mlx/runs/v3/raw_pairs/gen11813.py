from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([5, 26, 29, 37]))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("amount")).otherwise(0)).alias("cancelled_amount"),
    )
    .filter(F.col("cancelled_amount") < 54)
    .select("status", "cancelled_amount")
    .orderBy("status")
)
