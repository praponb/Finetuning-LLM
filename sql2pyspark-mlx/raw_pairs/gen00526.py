from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Acme", "Globex"]) & F.col("stock_qty").between(162, 394))
)
