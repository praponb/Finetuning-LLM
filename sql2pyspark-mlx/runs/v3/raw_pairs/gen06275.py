from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "SG", "US"]))
    .select("city", F.lit("supplier").alias("source"))
    .unionByName(df_stores.select("city", F.lit("store").alias("source")))
    .orderBy("source")
)
