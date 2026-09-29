from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
    .orderBy(F.col("id").desc())
)
