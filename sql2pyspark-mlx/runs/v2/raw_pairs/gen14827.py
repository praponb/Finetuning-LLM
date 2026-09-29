from pyspark.sql import functions as F

max_cost = df_products.filter(F.col("supplier_id").isin([13, 19, 23])).agg(F.max("cost")).first()[0]

result = (
    df_products
    .filter(F.col("cost") >= max_cost)
    .select("id", "cost")
    .orderBy("cost")
)
