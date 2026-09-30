from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["SMB", "Consumer"]) & ~F.col("is_active"))
    .select(
        "id",
        "credit_limit",
        F.last_day("signup_date").alias("signup_date_month_end"),
    )
)
