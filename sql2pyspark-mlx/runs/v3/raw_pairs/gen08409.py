from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([2, 3, 9]) & F.col("cost").isNull())
    .groupBy(
        "status",
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_delivered"),
        F.countDistinct("cost").alias("unique_orders"),
    )
)
