from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([11, 12, 15]) & F.col("store_id").isin([31, 33, 34]) & (F.col("shipping_fee") >= 21))
    .select(
        "id",
        "discount",
        "store_id",
        "amount",
        F.month("order_date").alias("order_date_month"),
        F.round((F.col("discount") * 0.9), 2).alias("adjusted_discount"),
    )
    .orderBy(F.col("order_date_month").desc())
)
