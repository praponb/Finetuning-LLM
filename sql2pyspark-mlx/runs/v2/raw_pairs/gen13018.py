from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "partner") | (F.year("order_date") >= 2023))
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.count("discount").alias("discount_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy("order_quarter")
)
