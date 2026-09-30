from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.when(F.year(F.col("o.order_date")) < 2024, "before_2024").when(F.year(F.col("o.order_date")) == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
        F.col("o.channel"),
    )
    .agg(F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"))
    .filter(F.col("avg_cost") >= 71)
)
