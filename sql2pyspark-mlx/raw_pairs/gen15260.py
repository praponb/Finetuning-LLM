from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(F.lit("supplier").alias("origin"), "country")
    .unionByName(df_sessions.select(F.lit("session").alias("origin"), "country"))
    .distinct()
)
