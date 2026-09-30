from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").isNull() & ((F.col("launch_date") >= "2023-09-01") | (F.col("stock_qty") >= 291)))
    .select("supplier_id", "brand", F.upper("brand").alias("brand_upper"))
)
