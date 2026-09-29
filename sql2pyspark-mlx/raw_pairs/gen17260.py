from pyspark.sql import functions as F

valid = (
    df_customers
    .filter(F.col("signup_date") >= F.to_date(F.lit("2023-08-28")))
    .select(
        "*",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        (F.col("age") - F.col("credit_limit")).alias("age_minus_credit_limit"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

result = (
    valid
    .groupBy("country")
    .pivot("segment", ["Consumer", "Corporate"])
    .agg(F.avg("age_minus_credit_limit").alias("total"))
)
