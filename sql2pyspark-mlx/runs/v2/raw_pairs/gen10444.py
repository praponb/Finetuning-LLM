from pyspark.sql import functions as F

base = df_shipments.filter(F.col("status") != "DELIVERED")

order_id_summary = (
    base
    .groupBy("order_id")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = order_id_summary
