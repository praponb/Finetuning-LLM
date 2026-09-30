from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue", F.lit("store_sale").alias("record_type"))
    .unionByName(
        df_orders
        .filter(F.col("amount") <= 1280)
        .select(F.col("shipping_fee").alias("revenue"), F.lit("order").alias("record_type"))
    )
    .distinct()
    .orderBy("revenue")
)
