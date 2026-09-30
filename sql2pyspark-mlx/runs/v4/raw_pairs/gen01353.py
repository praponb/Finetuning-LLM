from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "right")
    .filter(F.col("p.supplier_id").isin([11, 13]))
    .groupBy(F.col("p.brand"))
    .agg(F.min(F.col("ss.sale_date")).alias("first_sale_date"))
)
