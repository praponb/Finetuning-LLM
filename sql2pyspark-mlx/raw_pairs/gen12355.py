from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(252, 353) & (F.length("product_name") > 7))
    .select("supplier_id", "tags", "stock_qty")
)
