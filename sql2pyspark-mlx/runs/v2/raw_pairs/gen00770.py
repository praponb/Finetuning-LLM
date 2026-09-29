from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(F.year(F.col("o.order_date")).alias("order_year"))
    .agg(
        F.sort_array(F.collect_set(F.col("s.carrier"))).alias("carrier_list"),
        F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"),
    )
    .orderBy(F.col("order_year").desc())
)
