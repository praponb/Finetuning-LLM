from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(170, 357) & (F.col("stock_qty") < 264))
    .select("id", "brand")
)
