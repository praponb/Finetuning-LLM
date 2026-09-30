from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "Singapore")
    .select("segment")
    .distinct()
    .orderBy("segment")
)
