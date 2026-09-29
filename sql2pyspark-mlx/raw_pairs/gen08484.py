from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["category", "brand"], ["category"], []],
        F.col("d.category").alias("category"),
        F.col("d.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("e.revenue")), 2).alias("avg_revenue"),
        F.min(F.col("e.units")).alias("min_units"),
        F.grouping_id().alias("grouping_level"),
    )
)
