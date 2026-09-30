from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@GMAIL.COM"))
    .select(
        "city",
        "segment",
        F.when(F.col("segment").isin(["Consumer", "Corporate"]), "Tier 1").otherwise("Other").alias("segment_group"),
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
    )
)
