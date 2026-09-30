from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") >= 1100) & ((F.year("order_date") == 2025) | (F.col("shipping_fee") > 28)))
    .groupBy("status", "channel")
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
    )
)
