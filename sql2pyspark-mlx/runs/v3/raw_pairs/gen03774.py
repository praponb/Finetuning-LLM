from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("quantity", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("quantity") != 7)
        .select("quantity", "order_id")
    )
    .distinct()
    .orderBy("order_id")
)
