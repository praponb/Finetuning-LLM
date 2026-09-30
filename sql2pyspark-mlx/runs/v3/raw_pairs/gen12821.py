from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Electronics", "Books", "Sports"]) & (F.col("launch_date") < "2023-08-01"))
    .select("id", "price", "brand")
    .orderBy(F.col("price").desc())
)
