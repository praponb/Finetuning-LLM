from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("o.discount"),
        F.col("p.method"),
        F.col("p.status"),
        F.col("p.amount"),
        F.trim(F.col("p.status")).alias("clean_status"),
    )
    .orderBy(F.col("status").desc(), F.col("method").desc())
)
