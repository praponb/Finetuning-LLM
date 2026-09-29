from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(F.quarter(F.col("o.order_date")).alias("order_quarter"))
    .agg(
        F.sum(F.when(F.col("s.status") == "LOST", F.col("s.cost")).otherwise(0)).alias("lost_cost"),
    )
    .orderBy(F.col("lost_cost").desc(), F.col("order_quarter").desc())
    .limit(1)
)
