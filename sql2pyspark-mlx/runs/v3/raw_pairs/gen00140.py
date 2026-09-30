from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2025-02-01")
    .groupBy("order_id")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
    .orderBy(F.col("order_id").desc())
)
