from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "carrier",
        "status",
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.countDistinct("cost").alias("unique_orders"),
    )
    .orderBy("shipped_cohort", "carrier")
)
