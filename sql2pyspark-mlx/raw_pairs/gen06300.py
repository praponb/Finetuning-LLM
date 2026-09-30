from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy("city")
    .pivot("segment", ["SMB", "Corporate", "Consumer"])
    .agg(F.max("credit_limit"))
    .orderBy("city")
)
