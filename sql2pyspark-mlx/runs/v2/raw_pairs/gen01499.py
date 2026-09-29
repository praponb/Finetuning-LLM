from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("product_name").like("D%") & F.col("supplier_id").isNull() & (F.col("stock_qty") <= 243))
    .orderBy(F.col("id").desc())
    .limit(3)
)
