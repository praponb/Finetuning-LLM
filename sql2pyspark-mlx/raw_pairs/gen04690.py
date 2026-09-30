from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .filter(F.col("ss.store_id").isin([1, 2, 3, 4]) & F.col("p.supplier_id").isin([14, 27, 28]))
    .select(F.col("ss.id"), F.col("ss.sale_date"), F.col("p.supplier_id"))
)
