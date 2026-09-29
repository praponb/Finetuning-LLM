from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower("name").like("%ob %") & F.col("city").isNull())
    .select(
        "id",
        "age",
        "email",
        "segment",
        F.round("credit_limit", 0).alias("credit_limit_rounded"),
    )
    .orderBy("email", F.col("credit_limit_rounded").desc(), "id")
    .limit(10)
)
