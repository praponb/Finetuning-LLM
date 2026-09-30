from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(~F.col("c.country").isin(["TH", "JP"]))
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("c.city"),
        F.col("c.is_active"),
        F.col("c.name"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
