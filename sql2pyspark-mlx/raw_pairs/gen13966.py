from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.count("*").alias("num_order_items"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
    .filter(F.col("unit_price_range") < 55)
    .select("order_id", "num_order_items", "unit_price_range")
    .orderBy(F.col("unit_price_range").desc())
)
