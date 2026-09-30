from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "rating", "country", F.length("country").alias("country_length"))
)
