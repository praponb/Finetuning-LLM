from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["Singapore", "New York"])
    .agg(F.count("credit_limit"))
    .orderBy("country")
)
