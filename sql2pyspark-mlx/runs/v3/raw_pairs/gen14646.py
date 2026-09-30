from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .select(
        "id",
        "name",
        "signup_date",
        "credit_limit",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.concat("segment", F.lit(" - "), "country").alias("segment_country"),
    )
)
