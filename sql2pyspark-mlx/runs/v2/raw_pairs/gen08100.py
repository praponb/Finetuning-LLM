from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "LOST", "RETURNED"]) & F.col("order_id").isin([3, 19, 32, 36]) & (F.col("shipped_date") < "2023-08-01"))
    .groupBy("status")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.sum("cost").alias("total_cost"),
    )
    .orderBy(F.col("status").desc())
)
