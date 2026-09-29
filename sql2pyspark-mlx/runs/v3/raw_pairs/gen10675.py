from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("o.amount") >= 910)
    .groupBy(F.col("o.channel"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.status")))).alias("status_values"),
        F.count(F.when(F.col("s.status") == "RETURNED", 1)).alias("returned_count"),
    )
    .orderBy(F.col("returned_count").desc(), "channel")
)
