from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper("name").like("%ARO%"))
    .select(
        "id",
        "signup_date",
        "segment",
        "age",
        (F.col("age") - F.col("credit_limit")).alias("age_minus_credit_limit"),
    )
)
