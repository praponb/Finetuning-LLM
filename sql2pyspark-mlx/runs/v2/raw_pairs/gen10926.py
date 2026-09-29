from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("channel").isin(["mobile", "partner", "web"]) & (F.col("status") != "SHIPPED"))
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
        F.countDistinct("discount").alias("unique_channels"),
    )
)
