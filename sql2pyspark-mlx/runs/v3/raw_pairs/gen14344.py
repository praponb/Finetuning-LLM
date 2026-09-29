from pyspark.sql import functions as F

result = (
    df_customers
    .select("name", F.lit("customer").alias("source"))
    .union(df_employees.select("full_name", F.lit("employee").alias("source")))
    .orderBy("source")
)
