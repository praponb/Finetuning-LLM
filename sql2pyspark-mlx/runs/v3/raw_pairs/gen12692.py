from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 5)
    .select("quantity", "product_id")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price").isNotNull())
        .select("quantity", "product_id")
    )
    .orderBy(F.col("product_id").desc())
)
