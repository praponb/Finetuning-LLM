from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select("id", "name", "age", F.substring("country", 1, 4).alias("country_prefix"))
)
