from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("p.status") == "FAILED")
    .groupBy(
        F.month(F.col("s.opened_date")).alias("opened_month"),
        F.when(F.col("s.sqft") >= 17868, "high").when(F.col("s.sqft") >= 4183, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.status")))).alias("status_values"),
        F.min(F.col("p.amount")).alias("min_amount"),
    )
)
