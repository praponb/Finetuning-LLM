from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total_2"),
    )
    .orderBy("id")
    .limit(5)
)
