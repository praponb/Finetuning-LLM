from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") != "DE")
    .withColumn("is_de", F.when(F.col("country") == "DE", 1).otherwise(0))
    .withColumn("browser_lower", F.lower("browser"))
)
