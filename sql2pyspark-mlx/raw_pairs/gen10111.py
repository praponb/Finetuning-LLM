from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter(F.col("order_id").isin([11, 19, 34]) | F.month("shipped_date").isin([4, 5, 12]))
)

shipments_stats = (
    valid_shipments
    .groupBy(
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
    )
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = shipments_stats.filter(F.col("avg_cost") < 49)
