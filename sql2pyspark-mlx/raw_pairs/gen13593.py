from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "email",
        "signup_date",
        F.when(F.col("age") >= 73, "High").when(F.col("age") >= 26, "Medium").otherwise("Low").alias("age_level"),
    )
)
