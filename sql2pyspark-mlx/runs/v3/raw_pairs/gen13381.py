from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") != "London")
    .select("country", "segment")
)
