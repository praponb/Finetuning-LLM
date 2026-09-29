from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["Chiang Mai", "Bangkok", "Tokyo"])
    .agg(F.avg("credit_limit"))
    .orderBy("country")
)
