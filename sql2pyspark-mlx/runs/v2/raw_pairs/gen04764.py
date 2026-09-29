from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 4))
    .select(
        "id",
        "quantity",
        "order_id",
        "unit_price",
        F.ceil("quantity").alias("quantity_whole"),
    )
    .orderBy(F.col("quantity").desc(), "id")
    .limit(1)
)
