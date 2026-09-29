from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Bangkok")
    .select("city", F.lit("supplier").alias("source"))
    .unionByName(df_stores.select("city", F.lit("store").alias("source")))
    .distinct()
)
