from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") >= 7)
    .select(
        "id",
        "unit_price",
        F.round("unit_price", 2).alias("unit_price_rounded"),
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
    .orderBy(F.col("unit_price_rounded").desc(), "id")
    .limit(10)
)
