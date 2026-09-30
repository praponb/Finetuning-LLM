from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg(
        F.count("cost").alias("cost_count"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("stddev_cost") >= 55)
    .select("shipped_cohort", "cost_count", "stddev_cost")
    .orderBy("shipped_cohort")
)
