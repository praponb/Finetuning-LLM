from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("p.brand").isin(["Umbrella", "Stark"]))
    .select(F.col("p.category"), F.col("p.stock_qty"), F.col("ss.revenue"))
)
