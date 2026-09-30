from pyspark.sql import functions as F

flattened = df_app_logs.select("id", "service", "level", F.explode("context").alias("key", "v"))

result = (
    flattened
    .select("id", "service", "level", "key", "v")
    .orderBy(F.col("level").desc(), "id")
)
