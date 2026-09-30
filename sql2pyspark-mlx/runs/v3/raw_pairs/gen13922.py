from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.2, 3.1))
    .select("id", "city", F.substring("country", 1, 3).alias("country_prefix"))
)
