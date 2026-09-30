from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Consumer", "SMB", "Corporate"]))
    .select(
        "id",
        "name",
        "signup_date",
        "credit_limit",
        F.round((F.col("age") / F.col("credit_limit")), 2).alias("age_per_credit_limit"),
    )
)
