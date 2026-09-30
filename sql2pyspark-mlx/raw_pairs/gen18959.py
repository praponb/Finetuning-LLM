from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(75, 94) & (F.col("order_id") == 31))
    .groupBy(
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .orderBy("shipped_cohort")
)
