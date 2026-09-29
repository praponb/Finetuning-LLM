from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("shipping_fee") <= 39) & (F.col("shipping_fee") <= 27)) | (F.col("status") == "COMPLETED"))
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)
