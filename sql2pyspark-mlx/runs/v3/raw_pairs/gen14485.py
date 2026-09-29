from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") > 790) & ((F.col("order_date") > "2025-03-18") | F.col("customer_id").isin([15, 21])))
    .groupBy("customer_id")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
)
