from pyspark.sql import functions as F

result = (
    df_carts
    .select("id", "customer_id", F.explode_outer("attributes").alias("key", "value"))
)
