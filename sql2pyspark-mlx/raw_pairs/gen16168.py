from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select(
        "id",
        "signup_date",
        "age",
        F.date_add("signup_date", 81).alias("signup_date_plus_81d"),
    )
    .orderBy(F.col("age").desc(), F.col("signup_date").desc())
)
