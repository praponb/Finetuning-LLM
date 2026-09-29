from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("c.country") == "TH")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.amount"),
        F.col("c.name"),
        F.col("c.signup_date"),
        F.col("s.opened_date"),
    )
    .orderBy("opened_date")
)
