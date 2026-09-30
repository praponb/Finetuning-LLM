from pyspark.sql import functions as F

filtered_customers = df_customers.filter(~F.col("country").isin(["JP", "UK", "TH"]))

country_summary = (
    filtered_customers
    .groupBy("country")
    .agg((F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"))
)

result = (
    country_summary
    .select("country", "credit_limit_range")
    .orderBy(F.col("country").desc())
)
