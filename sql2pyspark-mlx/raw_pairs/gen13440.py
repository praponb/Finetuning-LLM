from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "JP")
    .select("age", F.upper("segment").alias("segment_upper"))
)
