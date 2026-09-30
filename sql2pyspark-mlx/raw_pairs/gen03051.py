from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("ss.revenue") < 1400) | F.col("p.brand").isin(["Stark", "Initech"]))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("p.brand"),
        F.col("p.price"),
        F.col("p.category"),
        F.floor(F.col("ss.units")).alias("units_whole"),
    )
)
