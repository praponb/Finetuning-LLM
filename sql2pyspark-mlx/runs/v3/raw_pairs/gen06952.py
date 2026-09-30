from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.upper(F.trim("name")) == "FRANK MILLER") & F.col("city").isNotNull() & F.col("city").isNotNull())
    .withColumn("is_tokyo", F.when(F.col("city") == "Tokyo", 1).otherwise(0))
)

agg = (
    base
    .groupBy("city", "segment")
    .agg(
        F.max("is_tokyo").alias("max_is_tokyo"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
)

result = (
    agg
    .filter(F.col("max_credit_limit") < 9600)
    .select("city", "segment", "max_is_tokyo", "max_credit_limit")
    .orderBy(F.col("city").desc())
)
