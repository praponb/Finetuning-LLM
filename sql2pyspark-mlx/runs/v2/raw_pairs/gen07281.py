from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("credit_limit_filled", F.coalesce("credit_limit", F.lit(0)))
    .orderBy(F.col("segment").desc(), "id")
    .limit(10)
)
