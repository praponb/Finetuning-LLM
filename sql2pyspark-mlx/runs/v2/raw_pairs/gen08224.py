from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNotNull() & ((F.col("shipping_fee") <= 38) | (F.col("order_date") > "2023-11-01")))
    .select("id", "store_id", F.col("discount").cast("int").alias("discount_int"))
    .orderBy(F.col("store_id").desc(), "id")
    .limit(50)
)
