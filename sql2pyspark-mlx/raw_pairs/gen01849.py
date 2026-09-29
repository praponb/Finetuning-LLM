from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("unit_price").between(250, 340))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
        (F.col("unit_price") * F.col("quantity")).alias("line_total_2"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("line_total") >= 81, "HIGH").otherwise("LOW").alias("line_total_tier"),
    )
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.count("*").alias("cnt"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.percentile_approx("line_total_2", 0.9).alias("line_total_2_p90"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .orderBy(F.col("line_total_2_p90").desc())
)
