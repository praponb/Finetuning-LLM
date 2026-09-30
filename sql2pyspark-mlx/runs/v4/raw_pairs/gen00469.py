from pyspark.sql import functions as F

src = df_customers.filter(F.col("is_active"))

agg = (
    src
    .groupBy("country")
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
)

result = (
    agg
    .filter(F.col("avg_credit_limit") < 64)
    .select("country", "avg_credit_limit")
    .orderBy("country")
)
