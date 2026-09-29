from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city", "rating", "country", F.round("rating", 1).alias("rating_rounded"))
)
