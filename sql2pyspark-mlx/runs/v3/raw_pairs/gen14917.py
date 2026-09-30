from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "UK")
    .select("country", F.lit("supplier").alias("source"))
    .unionByName(df_sessions.select("country", F.lit("session").alias("source")))
    .orderBy(F.col("country").desc())
)
