from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/product")
    .withColumn("is_home", F.when(F.col("page") == "/home", 1).otherwise(0))
)
