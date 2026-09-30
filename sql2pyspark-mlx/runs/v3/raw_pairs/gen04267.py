from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("name") < 5)
    .select(
        "id",
        "credit_limit",
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
        F.add_months("signup_date", 1).alias("signup_date_plus_months"),
    )
)
