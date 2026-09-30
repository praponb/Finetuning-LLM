from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([1, 3, 8]) & (F.col("signup_date") >= "2023-10-01") & (F.lower(F.trim("name")) == "emma brown"))
    .select(
        "id",
        "name",
        F.lower("name").alias("name_lower"),
        F.date_sub("signup_date", 12).alias("signup_date_minus_12d"),
    )
)
