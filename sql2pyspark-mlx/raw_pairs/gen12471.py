from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") > 130)
    .select("id", "cost", "launch_date")
)
