from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") > 110))
    .select(
        "*",
        F.when(F.col("unit_price") >= 400, "High").when(F.col("unit_price") >= 130, "Medium").otherwise("Low").alias("unit_price_level"),
    )
)

result = (
    filtered
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.unit_price")).alias("max_unit_price"),
        F.grouping_id().alias("gid"),
    )
)
