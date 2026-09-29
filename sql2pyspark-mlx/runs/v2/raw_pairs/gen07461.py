from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.month(F.col("p.launch_date")).isin([4, 10, 11]) & F.month(F.col("ss.sale_date")).isin([1, 3, 9]))
    .select(F.col("ss.id"), F.col("ss.store_id"), F.col("ss.units"), F.col("p.cost"))
)
