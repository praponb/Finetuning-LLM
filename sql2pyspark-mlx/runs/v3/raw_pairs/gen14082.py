from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select("id", "quantity", "product_id")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price") >= 140)
        .select("id", "quantity", "product_id")
    )
)
