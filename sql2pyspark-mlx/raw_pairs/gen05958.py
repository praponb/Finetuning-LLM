from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([12, 18, 24, 33]))
    .withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
    .withColumn("line_total", (F.col("quantity") + F.col("unit_price")))
)

result = (
    cleaned
    .select("product_id", "id")
    .exceptAll(
        df_order_items
        .filter(F.col("quantity") != 5)
        .select("product_id", "id")
    )
)
