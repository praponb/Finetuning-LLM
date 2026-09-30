from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.sqft") <= 15690)
    .groupBy(F.col("s.city"), F.col("s.region"))
    .agg(
        F.count(F.col("o.discount")).alias("discount_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("o.status")))).alias("status_values"),
    )
    .orderBy(F.col("status_values").desc())
)
