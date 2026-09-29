from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "customer_id", F.explode("attributes").alias("attr_key", "value"))
)

result = (
    exploded
    .select("id", "attr_key")
    .orderBy("attr_key", F.col("id").desc())
)
