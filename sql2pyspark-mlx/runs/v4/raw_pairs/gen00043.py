from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") != "UK") | (F.col("device") == "desktop"))
    .withColumn("clean_browser", F.trim("browser"))
    .withColumn("is_tablet", F.when(F.col("device") == "tablet", 1).otherwise(0))
)
