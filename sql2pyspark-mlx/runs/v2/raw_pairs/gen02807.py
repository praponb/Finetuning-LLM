from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.segment") == "SMB")
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"), F.col("c.city"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.status") == "IN_TRANSIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_in_transit"),
    )
    .orderBy(F.col("pct_in_transit").desc())
)
