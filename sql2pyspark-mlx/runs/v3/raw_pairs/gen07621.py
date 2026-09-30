from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("supplier_id").isNull() & (F.col("stock_qty") != 317)) | (F.col("cost") >= 540))
    .select("id", "cost")
)
