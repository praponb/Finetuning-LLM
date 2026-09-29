from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower(F.trim("supplier_name")) == "pacific wholesale")
    .select("city", F.lit("supplier").alias("source"))
    .unionByName(df_stores.select("city", F.lit("store").alias("source")))
    .distinct()
)
