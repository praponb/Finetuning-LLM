from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("country").isin(["DE", "TH", "US"]))
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg((F.max("age") - F.min("age")).alias("age_range"))
    .filter(F.col("age_range") < 64)
)
