from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("name") >= 13)
    .select("segment")
    .intersect(df_customers.filter(F.col("age") < 50).select("segment"))
)
