from pyspark.sql import functions as F

result = (
    df_customers
    .select("age")
    .orderBy("age")
)
