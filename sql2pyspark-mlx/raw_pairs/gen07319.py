from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "stock_qty", F.least("cost", "price").alias("least_cost_price"))
)
