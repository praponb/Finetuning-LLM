from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "UK")
    .select("city", "country")
    .distinct()
    .orderBy("city")
)
