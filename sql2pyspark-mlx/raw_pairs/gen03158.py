from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("discount")).otherwise(0)).alias("completed_discount"),
    )
    .filter(F.col("amount_range") > 72)
)
