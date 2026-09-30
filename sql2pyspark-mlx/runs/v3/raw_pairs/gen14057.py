from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("stock_qty") > 104))
    .orderBy("product_name")
)
