from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") < "2024-03-19") & ~F.col("is_active"))
    .select(
        "id",
        "age",
        "country",
        (F.col("age") + F.col("credit_limit")).alias("age_plus_credit_limit"),
    )
    .orderBy(F.col("id").desc())
)
