from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNull())
    .select("product_id")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price").isNotNull())
        .select("product_id")
    )
    .distinct()
    .orderBy("product_id")
)
