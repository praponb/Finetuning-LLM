from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & F.col("country").isin(["UK", "TH", "SG"]))
    .select(
        "country",
        "signup_date",
        "credit_limit",
        F.upper("country").alias("country_upper"),
    )
)
