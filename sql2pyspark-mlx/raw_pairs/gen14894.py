from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("p.status") == "SUCCESS") & (F.col("p.amount") < 1460))
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("o.customer_id"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.add_months(F.col("s.opened_date"), 6).alias("opened_date_plus_months"),
    )
)
