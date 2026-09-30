from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNull())
    .select("id", "quantity", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("quantity") == 7)
        .select("id", "quantity", "product_id")
    )
    .orderBy("product_id")
)
