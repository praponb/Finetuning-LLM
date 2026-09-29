from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "DE")
    .orderBy("segment", "name", "id")
    .limit(10)
)
