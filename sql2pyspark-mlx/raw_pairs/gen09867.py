from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.round(F.avg(F.col("o.discount")), 2).alias("avg_discount"),
        F.sum(F.col("o.discount")).alias("total_discount"),
    )
    .orderBy(F.col("signup_year").desc())
)
