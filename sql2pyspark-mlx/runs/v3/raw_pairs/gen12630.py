from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "city")
    .orderBy("city", F.col("id").desc())
)
