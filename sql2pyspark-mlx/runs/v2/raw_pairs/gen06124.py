from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["COMPLETED", "RETURNED"]))
    .groupBy(F.year("order_date").alias("order_year"), "store_id")
    .agg(
        F.min("order_date").alias("min_order_date"),
        F.count("shipping_fee").alias("shipping_fee_count"),
    )
    .filter(F.col("shipping_fee_count") < 8)
    .orderBy(F.col("shipping_fee_count").desc(), "order_year", "store_id")
    .limit(20)
)
