from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(730, 750) & F.col("supplier_id").isNull())
    .select("stock_qty", "price")
)
