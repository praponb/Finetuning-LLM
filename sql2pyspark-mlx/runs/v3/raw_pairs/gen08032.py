from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["US", "TH", "UK"]))
    .groupBy("country")
    .pivot("city", ["Berlin", "Singapore", "New York", "Tokyo"])
    .agg(F.count("age"))
)
