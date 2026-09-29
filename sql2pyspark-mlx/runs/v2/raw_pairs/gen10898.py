from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Grocery", "Sports"]) & (F.col("price") > 470))
    .select("id", "stock_qty", "cost")
)
