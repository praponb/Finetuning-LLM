from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("shipped_date") > "2023-06-01")

delivered_cohort_summary = (
    recent_shipments
    .groupBy(
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
    )
    .agg(F.max("delivered_date").alias("latest_delivered_date"))
)

result = delivered_cohort_summary.orderBy(F.col("delivered_cohort").desc())
