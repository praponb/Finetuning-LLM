from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") > 45)
    .select("id", "name", "segment", "age")
)
