from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(
        F.when(F.year(F.col("o.order_date")) < 2024, "before_2024").when(F.year(F.col("o.order_date")) == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.min(F.col("s.cost")).alias("min_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.carrier")))).alias("carrier_values"),
    )
    .orderBy(F.col("min_cost").desc(), "order_cohort")
    .limit(1)
)
