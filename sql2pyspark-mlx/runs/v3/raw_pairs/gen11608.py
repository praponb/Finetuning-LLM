from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("discount")).otherwise(0)).alias("completed_discount"),
    )
    .orderBy(F.col("completed_discount").desc())
)
