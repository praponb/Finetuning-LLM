from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") != 3)
    .select(
        "id",
        "order_id",
        "quantity",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
    .orderBy(F.col("quantity").desc())
)
