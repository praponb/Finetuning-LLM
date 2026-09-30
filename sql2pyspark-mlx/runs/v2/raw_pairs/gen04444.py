from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city", F.lit("supplier").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("city") == "Bangkok")
        .select("city", F.lit("store").alias("source"))
    )
    .distinct()
)
