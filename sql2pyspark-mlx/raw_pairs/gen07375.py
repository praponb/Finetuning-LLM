from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.month(F.col("s.shipped_date")).isin([1, 7, 10]))
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.max(F.col("s.cost")).alias("max_cost"),
        (F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"),
    )
    .filter(F.col("max_cost") >= 41)
    .orderBy(F.col("signup_year").desc())
    .limit(10)
)
