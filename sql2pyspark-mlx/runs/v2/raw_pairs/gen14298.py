from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["PENDING", "COMPLETED"]) & (F.col("store_id") == 29))
    .groupBy("channel", "store_id")
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
    )
    .orderBy("web_count")
)
