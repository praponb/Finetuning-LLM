from pyspark.sql import functions as F

src = (
    df_order_items
    .filter((F.col("unit_price") <= 310) & F.col("unit_price").between(100, 250))
    .withColumn("line_total", F.round((F.col("unit_price") / F.col("quantity")), 2))
)

order_id_summary = (
    src
    .groupBy("order_id", "product_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.count("line_total").alias("line_total_count"),
    )
)

result = (
    order_id_summary
    .select("order_id", "unit_price_range", "line_total_count")
    .orderBy(F.col("unit_price_range").desc(), F.col("line_total_count").desc())
)
