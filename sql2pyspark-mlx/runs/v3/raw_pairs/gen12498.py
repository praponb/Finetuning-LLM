from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price") > 400)
        .select(F.col("unit_price").alias("revenue"))
    )
    .orderBy("revenue")
)
