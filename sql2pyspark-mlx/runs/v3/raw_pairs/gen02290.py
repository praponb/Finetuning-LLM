from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.col("o.channel"),
        F.quarter(F.col("o.order_date")).alias("order_quarter"),
    )
    .agg(
        F.sum(F.col("s.cost")).alias("total_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.status")))).alias("status_values"),
    )
    .filter(F.col("total_cost") >= 300)
    .orderBy("order_quarter", "channel")
    .limit(5)
)
