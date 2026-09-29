from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["London", "Chiang Mai", "Bangkok"])
    .agg(F.avg("age"))
    .orderBy("country")
)
