from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 6)
    .select("id", "product_id")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price") > 220)
        .select("id", "product_id")
    )
    .orderBy("product_id")
)
