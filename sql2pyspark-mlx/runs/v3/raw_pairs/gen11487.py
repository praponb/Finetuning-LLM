from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", F.lit("customer").alias("source"))
    .unionByName(df_suppliers.select("country", F.lit("supplier").alias("source")))
)
