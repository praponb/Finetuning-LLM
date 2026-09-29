from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(54, 67))
    .groupBy("is_active")
    .pivot("city", ["Tokyo", "Chiang Mai", "Singapore", "London"])
    .agg(F.avg("age"))
    .orderBy("is_active")
)
