from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("city").isin(["New York", "Chiang Mai", "Singapore"]))
    .select(
        "id",
        "signup_date",
        "credit_limit",
        "is_active",
        F.when(F.col("country") == "DE", 1).otherwise(0).alias("is_de"),
    )
    .orderBy(F.col("is_de").desc(), "signup_date")
)
