from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 10500)
    .groupBy("segment")
    .pivot("country", ["UK", "TH"])
    .agg(F.sum("age"))
)
