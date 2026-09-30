from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 10600)
    .select(
        "id",
        "country",
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
        (F.col("age") - F.col("credit_limit")).alias("age_minus_credit_limit"),
    )
    .orderBy(F.col("age_minus_credit_limit").desc())
)
