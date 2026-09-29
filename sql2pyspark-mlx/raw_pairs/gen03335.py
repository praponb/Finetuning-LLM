from pyspark.sql import functions as F

result = (
    df_products
    .filter(~(F.col("stock_qty").between(169, 356) & (F.col("launch_date") < "2023-11-01")))
    .select("id", "cost", "tags")
)
