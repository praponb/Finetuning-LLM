from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Initech", "Acme", "Umbrella"]))
    .select("id", "price", "product_name", F.explode("tags").alias("label"))
)
