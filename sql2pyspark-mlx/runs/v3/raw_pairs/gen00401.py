from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") <= 14500) & (F.col("signup_date") <= "2025-09-14"))
    .select(
        "id",
        "segment",
        "is_active",
        F.round((F.col("credit_limit") / F.col("age")), 2).alias("credit_limit_per_age"),
    )
    .orderBy("id")
    .limit(1)
)
