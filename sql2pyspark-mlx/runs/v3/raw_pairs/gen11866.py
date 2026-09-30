from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .unionByName(
        df_order_items
        .filter(F.col("order_id").isin([19, 31, 39]))
        .select(F.col("unit_price").alias("price"))
    )
)
