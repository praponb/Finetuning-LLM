from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "RETURNED") & F.col("store_id").isNotNull())
    .groupBy("channel")
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.max("order_date").alias("last_order_date"),
        F.max("order_date").alias("latest_order_date"),
    )
    .filter(F.col("discount_range") >= 78)
    .orderBy("last_order_date")
)
