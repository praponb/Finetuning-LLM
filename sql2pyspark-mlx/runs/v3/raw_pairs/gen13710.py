from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(F.count("*").alias("cnt"))
    .orderBy("cnt")
)
