from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") > "2024-05-01")
    .select("id", "stock_qty")
    .subtract(df_products.filter(F.col("price") < 1030).select("id", "stock_qty"))
)
