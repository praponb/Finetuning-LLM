from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .pivot("status", ["CANCELLED", "RETURNED"])
    .agg(F.sum("amount").alias("total"))
    .select(
        "store_id",
        F.col("CANCELLED").alias("cancelled"),
        F.col("RETURNED").alias("returned"),
    )
)
