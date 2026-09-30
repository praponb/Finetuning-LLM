from pyspark.sql import functions as F

result = (
    df_sessions
    .select("country", F.lit("session").alias("origin"))
    .union(df_suppliers.select("city", F.lit("supplier").alias("origin")))
    .distinct()
)
