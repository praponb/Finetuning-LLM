from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(
        F.count(F.col("o.discount")).alias("discount_count"),
        F.sum(F.when(F.col("o.channel") == "mobile", F.col("o.discount")).otherwise(0)).alias("mobile_discount"),
    )
    .orderBy("mobile_discount", F.col("opened_month").desc())
)
