from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter((F.col("c.segment") == "SMB") & F.col("s.store_name").like("Ri%"))
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.customer_id"),
        F.col("s.sqft"),
        F.col("c.signup_date"),
        F.col("c.country"),
    )
    .orderBy("sqft", "country")
)
