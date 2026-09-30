from pyspark.sql import functions as F

src = df_orders.filter(F.col("channel") == "partner")

summary = (
    src
    .groupBy("channel")
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
        F.sum("discount").alias("total_discount"),
    )
)

result = (
    summary
    .filter(F.col("total_discount") > 2.2)
    .orderBy(F.col("shipped_count").desc(), "channel")
    .limit(10)
)
