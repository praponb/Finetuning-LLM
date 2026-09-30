from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.channel").isNotNull())
    .groupBy(F.quarter(F.col("o.order_date")).alias("order_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.method")))).alias("method_values"),
        F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"),
    )
    .orderBy(F.col("method_values").desc(), F.col("order_quarter").desc())
)
