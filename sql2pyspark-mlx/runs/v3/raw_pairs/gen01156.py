from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"),
        F.round(((100 * F.sum(F.when(F.col("s.carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
    )
)
