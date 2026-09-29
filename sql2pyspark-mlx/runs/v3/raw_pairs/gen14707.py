from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["SMB", "Corporate"]) & (F.col("age") >= 33))
    .select(
        "id",
        "age",
        "signup_date",
        "city",
        F.round((F.col("credit_limit") * 0.9), 2).alias("credit_limit_adjusted"),
    )
)
