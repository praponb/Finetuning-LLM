from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["DE", "SG", "US"]))
    .select("id", "city", "rating", "country", F.ceil("rating").alias("rating_whole"))
    .orderBy("rating_whole", "rating")
)
