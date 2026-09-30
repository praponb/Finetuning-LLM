from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") > 10900) & F.col("signup_date").between("2024-08-01", "2025-10-01"))
    .select(
        "id",
        "segment",
        "signup_date",
        "name",
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
    .orderBy(F.col("is_active_flag").desc(), F.col("id").desc())
    .limit(3)
)
