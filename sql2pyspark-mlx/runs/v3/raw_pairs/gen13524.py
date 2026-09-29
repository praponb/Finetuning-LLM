from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNull())
    .select("country", F.lit("supplier").alias("source"))
    .unionByName(
        df_sessions
        .filter(F.col("started_at") >= "2025-10-29")
        .select("country", F.lit("session").alias("source"))
    )
)
