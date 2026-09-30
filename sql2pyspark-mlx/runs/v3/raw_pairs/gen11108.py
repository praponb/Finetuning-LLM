from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", "order_id", F.month("shipped_date").alias("shipped_month"))
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("max_cost") >= 55)
    .drop("max_cost")
    .orderBy(F.col("shipped_month").desc(), "status", "order_id")
    .limit(3)
)
