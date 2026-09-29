from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("segment", ["SMB", "Corporate"])
    .agg(F.max("credit_limit"))
)
