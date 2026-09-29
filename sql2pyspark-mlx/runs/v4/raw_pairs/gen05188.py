from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment").isin(["Corporate", "Consumer"]))
    .groupBy(
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
        F.col("c.is_active"),
    )
    .agg(F.round(F.avg(F.col("o.amount")), 2).alias("avg_amount"))
)
