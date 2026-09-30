from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.when(F.col("s.carrier") == "UPS", F.col("s.cost")).otherwise(0)).alias("ups_cost"),
    )
    .filter(F.col("ups_cost") > 74)
    .orderBy(F.col("status").desc())
    .limit(10)
)
