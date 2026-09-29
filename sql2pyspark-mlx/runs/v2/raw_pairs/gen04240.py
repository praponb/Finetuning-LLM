from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("supplier_id").isNotNull() & (F.col("stock_qty") != 200)) | F.array_contains("tags", "clearance"))
    .select("brand", "supplier_id", F.round("stock_qty", 2).alias("stock_qty_rounded"))
)
