from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 430)
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
        F.sum("discount").alias("total_discount"),
    )
    .orderBy("partner_count", "total_discount", "status")
    .limit(5)
)
