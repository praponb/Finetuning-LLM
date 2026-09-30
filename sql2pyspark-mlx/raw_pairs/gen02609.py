from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "US")
    .select("country")
    .orderBy("country")
)
