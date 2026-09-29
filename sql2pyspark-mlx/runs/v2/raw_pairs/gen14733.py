from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .filter(F.col("products.brand").isin(["Acme", "Umbrella", "Globex"]))
    .groupBy(F.col("products.category"))
    .agg(
        (F.max(F.col("store_sales.units")) - F.min(F.col("store_sales.units"))).alias("units_range"),
    )
    .orderBy("units_range", "category")
    .limit(10)
)
