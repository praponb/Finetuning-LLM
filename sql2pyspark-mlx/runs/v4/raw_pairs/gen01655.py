from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["Bangkok", "Berlin"])
    .agg(F.sum("age"))
)
