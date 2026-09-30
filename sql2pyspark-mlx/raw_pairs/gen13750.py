from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("signup_date") > "2025-01-07") & (F.length("email") > 7)) | F.col("signup_date").between("2024-06-16", "2024-09-01"))
    .select(
        "name",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
        F.coalesce("segment", F.lit("unassigned")).alias("segment_filled"),
    )
    .orderBy("segment_filled")
)
