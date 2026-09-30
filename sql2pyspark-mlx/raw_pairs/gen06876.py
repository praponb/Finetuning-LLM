from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.upper(F.trim(F.col("s.store_name"))) == "OLD TOWN")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("c.signup_date"),
        F.col("c.segment"),
    )
)
