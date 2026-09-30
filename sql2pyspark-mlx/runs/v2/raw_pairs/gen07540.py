from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "COMPLETED")
    .groupBy("channel", F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("mobile_count") < 57)
    .select("channel", "order_quarter", "mobile_count", "amount_count")
    .orderBy(F.col("order_quarter").desc())
)
