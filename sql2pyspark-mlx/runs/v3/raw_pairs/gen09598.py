from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select(
        "credit_limit",
        "signup_date",
        "country",
        F.initcap(F.trim("name")).alias("name_title"),
    )
)
