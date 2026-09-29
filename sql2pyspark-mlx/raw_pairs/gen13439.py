from pyspark.sql import functions as F

result = (
    df_sessions
    .select("browser", F.lit("session").alias("record_type"))
    .unionByName(
        df_suppliers
        .select(F.col("city").alias("browser"), F.lit("supplier").alias("record_type"))
    )
    .distinct()
    .orderBy("record_type")
)
