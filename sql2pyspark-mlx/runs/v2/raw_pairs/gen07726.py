from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("amount") >= 930) & F.col("store_id").isNull()) | (F.col("channel") != "partner"))
    .groupBy("channel")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.count("discount").alias("discount_count"),
    )
    .orderBy(F.col("discount_count").desc(), "channel")
)
