from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["New York", "Singapore"])
    .agg(F.count("age"))
    .orderBy("country")
)
