from pyspark.sql import functions as F

max_cost = df_products.agg(F.max("cost")).first()[0]

result = (
    df_products
    .filter(F.col("cost") >= max_cost)
    .select("id", "cost", "stock_qty", "launch_date")
    .orderBy(F.col("cost").desc(), F.col("stock_qty").desc(), "id")
    .limit(50)
)
