from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.col("segment").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Berlin"))))
    .select(
        "id",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

result = (
    filtered
    .select("segment", "city")
    .intersect(
        df_customers
        .filter(F.upper(F.trim("email")) == "KANYA@MAIL.CO.TH")
        .select("segment", "city")
    )
)
