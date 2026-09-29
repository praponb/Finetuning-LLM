from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("name").like("%Alic%") & F.col("name").isNotNull() & F.month("signup_date").isin([2, 5, 9]))
    .select(
        "id",
        "age",
        "signup_date",
        "email",
        F.round((F.col("age") * 1.15), 2).alias("age_scaled"),
        F.length("name").alias("name_length"),
    )
)
