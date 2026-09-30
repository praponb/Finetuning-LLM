from pyspark.sql import functions as F

result = (
    df_departments
    .select("location", F.lit("department").alias("source"))
    .union(df_stores.select("city", F.lit("store").alias("source")))
)
