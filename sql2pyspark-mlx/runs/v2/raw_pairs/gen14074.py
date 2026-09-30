from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(49, 52))
    .select("city", F.lit("customer").alias("source"))
    .unionByName(df_stores.select("city", F.lit("store").alias("source")))
    .orderBy("source")
)
