from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "name", F.substring("city", 1, 3).alias("city_prefix"))
    .orderBy("name")
)
