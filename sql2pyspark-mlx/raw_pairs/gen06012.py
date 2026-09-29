from pyspark.sql import functions as F

result = (
    df_customers
    .select("city", F.lit("customer").alias("origin"))
    .unionByName(df_stores.select("city", F.lit("store").alias("origin")))
    .distinct()
    .orderBy("origin")
)
