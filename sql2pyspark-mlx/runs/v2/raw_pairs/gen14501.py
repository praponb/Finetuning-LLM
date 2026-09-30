from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") > 57)
    .select(
        "id",
        "is_active",
        "email",
        F.round("credit_limit", 1).alias("credit_limit_rounded"),
    )
    .orderBy(F.col("credit_limit_rounded").desc(), F.col("email").desc(), "id")
    .limit(5)
)
