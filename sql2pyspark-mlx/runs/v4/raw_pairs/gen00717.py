from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter((F.col("p.status") != "FAILED") & F.col("o.status").isin(["RETURNED", "CANCELLED"]))
    .select(
        F.col("p.status"),
        F.col("p.order_id"),
        F.col("o.customer_id"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
    .orderBy(F.col("sqft").desc())
)
