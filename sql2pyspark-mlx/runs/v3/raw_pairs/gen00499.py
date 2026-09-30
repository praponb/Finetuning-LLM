from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "Chiang Mai") & (F.col("city").isNotNull() | (F.col("city") == "Tokyo")))
    .select("city", F.ceil("rating").alias("rating_whole"))
)
