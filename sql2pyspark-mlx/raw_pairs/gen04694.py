from pyspark.sql import functions as F

result = (
    df_customers
    .select(F.lit("customer").alias("source"), "city")
    .unionByName(df_stores.select(F.lit("store").alias("source"), "city"))
)
