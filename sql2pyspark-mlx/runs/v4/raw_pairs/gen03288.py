from pyspark.sql import functions as F

min_cost = df_products.agg(F.min("cost")).first()[0]

result = (
    df_products
    .filter(F.col("cost") == min_cost)
    .select("id", "cost", "category")
    .orderBy("category")
)
