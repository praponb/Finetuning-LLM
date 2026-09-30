from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["SMB", "Corporate"]) & (F.col("age") != 53))
    .select(
        "id",
        "signup_date",
        "is_active",
        "city",
        F.weekofyear("signup_date").alias("signup_date_weekofyear"),
        F.trim("name").alias("clean_name"),
    )
)
