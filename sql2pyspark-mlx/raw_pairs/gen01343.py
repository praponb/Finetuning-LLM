from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("segment") == "SMB") & F.col("email").like("%@gmail.com")) | F.col("is_active"))
    .select(
        "id",
        "segment",
        "signup_date",
        "email",
        F.coalesce("city", F.lit("unassigned")).alias("city_filled"),
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
