from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") == "New York")
    .select(F.col("ss.units"), F.col("p.cost"), F.col("p.brand"), F.col("s.sqft"))
)
