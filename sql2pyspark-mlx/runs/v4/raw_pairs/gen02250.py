from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .select("id", "product_id")
    .unionByName(
        df_order_items
        .filter(F.col("product_id").isNull())
        .select("id", "product_id")
    )
    .distinct()
)
