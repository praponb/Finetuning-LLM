from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(~F.col("s.region").isin(["North", "Central"]))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
        F.col("p.launch_date"),
        F.col("s.region"),
        F.col("s.sqft"),
    )
)
