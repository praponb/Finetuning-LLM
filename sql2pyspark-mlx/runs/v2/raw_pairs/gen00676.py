from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["New York", "Chiang Mai"])
    .agg(F.count("credit_limit"))
    .orderBy("country")
)
