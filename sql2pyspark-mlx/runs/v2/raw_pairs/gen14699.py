from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["Singapore", "Chiang Mai", "New York", "London"])
    .agg(F.count("credit_limit"))
    .orderBy("country")
)
