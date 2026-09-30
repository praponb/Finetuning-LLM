from pyspark.sql import functions as F

valid_shipments = df_shipments.filter(F.month("shipped_date").isin([1, 8, 11]))

shipments_stats = (
    valid_shipments
    .groupBy(
        F.quarter("delivered_date").alias("delivered_quarter"),
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
    )
)

result = (
    shipments_stats
    .filter(F.col("pct_fedex") > 37)
    .orderBy("delivered_quarter", "shipped_cohort")
    .limit(5)
)
