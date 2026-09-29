from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([18, 28]))
    .select("id", "tags", "brand", "stock_qty")
)
