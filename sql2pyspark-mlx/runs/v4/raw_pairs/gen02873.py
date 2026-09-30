from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("shipped_date") < "2025-08-01") & (F.col("cost") >= 38) & (F.col("delivered_date") >= "2023-07-01"))
)

summary = (
    valid_shipments
    .groupBy(
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
        F.min("shipped_date").alias("first_shipped_date"),
    )
)

result = summary.select("shipped_cohort", "pct_returned", "first_shipped_date")
