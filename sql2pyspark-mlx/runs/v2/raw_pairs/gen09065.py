from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
    .orderBy(F.col("quantity").desc(), F.col("id").desc())
)
