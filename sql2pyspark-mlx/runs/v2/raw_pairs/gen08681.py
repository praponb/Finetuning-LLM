from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "unit_price",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
