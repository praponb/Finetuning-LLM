from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id", "carrier")
    .pivot("status", ["LOST", "DELIVERED", "RETURNED"])
    .agg(F.avg("cost"))
    .select(
        "order_id",
        "carrier",
        F.col("LOST").alias("lost"),
        F.col("DELIVERED").alias("delivered"),
        F.col("RETURNED").alias("returned"),
    )
)
