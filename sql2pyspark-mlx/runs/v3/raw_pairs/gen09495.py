from pyspark.sql import functions as F

result = (
    df_order_items
    .select(F.col("unit_price").alias("value"), F.lit("order_item").alias("source"))
    .unionByName(
        df_store_sales
        .select(F.col("revenue").alias("value"), F.lit("store_sale").alias("source"))
    )
    .distinct()
    .orderBy("value")
)
