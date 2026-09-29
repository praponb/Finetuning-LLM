from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("order_id")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price").between(130, 230))
        .select("order_id")
    )
    .distinct()
)
