from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .filter(F.col("o.channel") == "mobile")
    .groupBy(F.quarter(F.col("o.order_date")).alias("order_quarter"))
    .agg(
        F.avg(F.col("s.cost")).alias("avg_cost"),
        F.sum(F.when(F.col("s.status") == "LOST", F.col("s.cost")).otherwise(0)).alias("lost_cost"),
    )
    .filter(F.col("avg_cost") > 81)
    .orderBy("lost_cost")
)
