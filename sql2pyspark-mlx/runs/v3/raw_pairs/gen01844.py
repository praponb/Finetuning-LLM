from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "SMB", "Consumer"]))
    .select(
        "id",
        "city",
        "credit_limit",
        "country",
        F.date_sub("signup_date", 37).alias("signup_date_minus_37d"),
    )
)
