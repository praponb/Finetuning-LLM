from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2024-04-26", "2025-05-01"))
    .select(
        "id",
        "country",
        "name",
        "city",
        F.round("credit_limit", 2).alias("credit_limit_rounded"),
    )
    .orderBy(F.col("id").desc(), "country")
)
