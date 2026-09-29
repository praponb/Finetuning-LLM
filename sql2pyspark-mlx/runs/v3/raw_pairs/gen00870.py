from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.is_active"))
    .select(
        F.col("c.credit_limit"),
        F.col("o.status"),
        F.col("o.discount"),
        F.concat(F.col("c.segment"), F.lit(" - "), F.col("c.name")).alias("segment_name"),
    )
    .orderBy(F.col("discount").desc(), "segment_name")
)
