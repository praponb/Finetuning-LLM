from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(41, 66))
    .select("country", "city")
    .distinct()
)
