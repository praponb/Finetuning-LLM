from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.col("s.cost")).alias("total_cost"),
        F.sum(F.when(F.col("s.carrier") == "Flash", F.col("s.cost")).otherwise(0)).alias("flash_cost"),
    )
    .orderBy(F.col("flash_cost").desc())
)
