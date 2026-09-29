from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "country",
        "signup_date",
        "is_active",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.concat("email", F.lit(", "), "segment").alias("email_segment"),
    )
)
