from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.segment").isin(["Consumer", "SMB"]))
    .groupBy(F.col("c.country"), F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(F.round(F.avg(F.col("o.discount")), 2).alias("avg_discount"))
    .filter(F.col("avg_discount") > 73)
)
