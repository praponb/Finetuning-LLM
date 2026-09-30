from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Backpac%"))
    .select("stock_qty")
    .unionByName(df_products.filter(F.col("brand") == "Initech").select("stock_qty"))
    .distinct()
)
