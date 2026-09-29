from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("rating", "city", "country", F.substring("city", 1, 2).alias("city_prefix"))
)
