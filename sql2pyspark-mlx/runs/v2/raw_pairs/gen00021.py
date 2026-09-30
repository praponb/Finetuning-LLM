from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("No%") & F.col("product_name").isNull() & F.array_contains("tags", "sale"))
    .groupBy("brand")
    .agg(
        F.min("cost").alias("min_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .filter(F.col("min_cost") >= 510)
)
