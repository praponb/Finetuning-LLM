from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter((F.col("o.discount") >= 0.16) & ~F.col("c.email").like("%@company.co"))
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("c.name"),
        F.col("c.country"),
        F.col("o.amount").cast("int").alias("amount_int"),
    )
    .orderBy(F.col("store_name").desc(), "id")
)
