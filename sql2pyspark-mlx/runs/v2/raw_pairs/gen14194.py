from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") != 32) | (F.col("age") < 48))
    .select("id", "country", F.upper("name").alias("name_upper"))
)
