from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(~F.col("s.city").isin(["Singapore", "London"]))
    .groupBy(
        F.when(F.col("s.city").isin(["Tokyo", "Bangkok"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(F.sum(F.col("p.amount")).alias("total_amount"))
)
