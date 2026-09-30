from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .pivot("segment", ["SMB", "Consumer", "Corporate"])
    .agg(F.max("credit_limit"))
)
