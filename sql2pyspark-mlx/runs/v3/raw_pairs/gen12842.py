from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower(F.trim("name")) == "david lee")
    .select("city", F.lit("customer").alias("source"))
    .unionByName(df_stores.select("city", F.lit("store").alias("source")))
    .distinct()
)
