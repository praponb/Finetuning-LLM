from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2023-09-29", "2024-04-25"))
    .select(
        "id",
        "segment",
        "email",
        F.when(F.col("city") == "Tokyo", 1).otherwise(0).alias("is_tokyo"),
    )
    .orderBy(F.col("is_tokyo").desc())
)
