from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country", F.lit("supplier").alias("record_type"))
    .unionByName(
        df_sessions
        .filter(F.col("browser").isin(["Firefox", "Safari"]))
        .select("country", F.lit("session").alias("record_type"))
    )
    .distinct()
)
