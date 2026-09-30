from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "DE")
    .select("city")
    .distinct()
    .orderBy("city")
)
