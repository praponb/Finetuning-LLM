from pyspark.sql import functions as F

src = df_order_items.filter((F.col("unit_price") < 310) & (F.col("unit_price") > 130))

order_id_summary = (
    src
    .groupBy("order_id", "product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)

result = (
    order_id_summary
    .filter(F.col("unit_price_range") < 74)
    .select("order_id", "product_id", "unit_price_range")
    .orderBy(F.col("unit_price_range").desc())
)
