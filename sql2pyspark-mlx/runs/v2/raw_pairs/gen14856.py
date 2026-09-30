from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%uro%"))
    .select("country", F.lit("supplier").alias("source"))
    .unionByName(df_sessions.select("country", F.lit("session").alias("source")))
    .distinct()
    .orderBy("source")
)
