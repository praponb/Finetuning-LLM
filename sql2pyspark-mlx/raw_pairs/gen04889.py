from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"), F.col("s.city"))
    .agg(
        F.sum(F.when(F.col("p.method") == "card", 1).otherwise(0)).alias("card_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("opened_month").desc())
)
