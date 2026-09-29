from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") <= "2025-06-01") & (F.col("order_date").between("2025-01-01", "2025-07-01") | (F.col("customer_id") == 11)))
    .groupBy(
        "status",
        F.when(F.col("shipping_fee") >= 38, "high").when(F.col("shipping_fee") >= 5, "mid").otherwise("low").alias("shipping_fee_bucket"),
        F.when(F.col("amount") >= 1220, "high").when(F.col("amount") >= 310, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
    .filter(F.col("returned_count") > 43)
)
