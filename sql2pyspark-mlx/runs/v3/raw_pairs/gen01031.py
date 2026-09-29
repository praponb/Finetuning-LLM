from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", "city")
    .distinct()
    .orderBy("city")
)
