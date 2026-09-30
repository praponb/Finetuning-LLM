from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("c.city").isin(["Tokyo", "Bangkok", "Chiang Mai"]))
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("s.sqft"),
        F.col("c.credit_limit"),
        F.col("c.country"),
    )
    .orderBy(F.col("sqft").desc())
)
