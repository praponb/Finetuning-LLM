from pyspark.sql import functions as F

src = df_customers.filter((F.col("credit_limit") <= 8700) & (F.length("email") > 12))

summary = (
    src
    .groupBy("segment")
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
)

result = (
    summary
    .filter(F.col("total_credit_limit") > 31000)
    .orderBy("total_credit_limit")
)
