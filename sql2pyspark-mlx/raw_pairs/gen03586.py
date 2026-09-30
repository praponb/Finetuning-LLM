from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter((F.col("p.amount") > 780) | F.col("p.order_id").isNotNull())
    .select(
        F.col("p.amount"),
        F.col("p.method"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("o.discount"),
    )
    .orderBy(F.col("method").desc())
)
