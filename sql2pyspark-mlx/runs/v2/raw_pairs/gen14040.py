from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["Bangkok", "Chiang Mai"])
    .agg(F.avg("credit_limit"))
    .orderBy("country")
)
