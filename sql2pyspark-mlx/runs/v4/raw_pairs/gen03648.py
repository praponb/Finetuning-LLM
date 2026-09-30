from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "carrier",
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg(
        F.sum("cost").alias("total_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .filter(F.col("total_cost") >= 440)
    .select("carrier", "shipped_cohort", "total_cost", "avg_cost")
)
