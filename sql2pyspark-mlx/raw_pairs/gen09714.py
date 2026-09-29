from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country", F.lit("supplier").alias("source"))
    .unionByName(
        df_sessions
        .filter(F.col("browser").isNotNull())
        .select("country", F.lit("session").alias("source"))
    )
)
