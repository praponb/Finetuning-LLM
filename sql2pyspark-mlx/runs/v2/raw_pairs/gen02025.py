from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .select("product_id")
    .unionByName(
        df_order_items
        .filter(F.col("order_id").isin([21, 26]))
        .select("product_id")
    )
    .orderBy("product_id")
)
