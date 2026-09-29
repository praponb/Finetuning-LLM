from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") >= 290) & (F.col("stock_qty") >= 243))
    .select("cost", "brand", "supplier_id")
)
