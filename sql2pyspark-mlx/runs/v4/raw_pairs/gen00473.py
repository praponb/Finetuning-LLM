from pyspark.sql import functions as F

src = (
    df_customers
    .filter(F.col("name").like("O%") & (F.length("email") > 6))
    .withColumn("signup_date_plus_85d", F.date_add("signup_date", 85))
)

agg = (
    src
    .groupBy("country")
    .agg(F.max("credit_limit").alias("max_credit_limit"))
)

result = (
    agg
    .orderBy(F.col("max_credit_limit").desc(), "country")
    .limit(20)
)
