from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand").isin(["Umbrella", "Globex"]) & F.array_contains("tags", "premium")) | F.col("supplier_id").isin([2, 8, 24, 27]))
    .groupBy("supplier_id")
    .agg((F.max("price") - F.min("price")).alias("price_range"))
    .filter(F.col("price_range") > 26)
)
