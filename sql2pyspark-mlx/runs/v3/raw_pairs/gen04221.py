from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("segment").isNull() & F.col("email").like("%@yahoo.com"))

agg = (
    filtered_customers
    .groupBy("segment", "city")
    .agg(F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"))
)

result = (
    agg
    .filter(F.col("de_count") < 8)
    .orderBy("segment", F.col("de_count").desc(), "city")
    .limit(50)
)
