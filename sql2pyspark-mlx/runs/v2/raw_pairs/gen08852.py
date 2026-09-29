from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select("quantity")
    .unionByName(
        df_order_items
        .filter(F.col("product_id").isNotNull())
        .select("quantity")
    )
    .distinct()
)
