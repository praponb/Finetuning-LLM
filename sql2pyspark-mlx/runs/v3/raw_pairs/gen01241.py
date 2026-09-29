from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.length("product_name") > 11) & (F.col("stock_qty") >= 188) & (F.size("tags") >= 2))
    .select("id", "launch_date", "price", "supplier_id")
)
