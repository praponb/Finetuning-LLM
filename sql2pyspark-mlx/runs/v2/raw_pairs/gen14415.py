from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(254, 274) & F.col("category").isin(["Fashion", "Home", "Grocery"]))
    .select("id", "product_name", "supplier_id", "brand")
)
