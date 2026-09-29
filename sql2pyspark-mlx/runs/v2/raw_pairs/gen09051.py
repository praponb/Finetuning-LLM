from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.amount").between(650, 1460) & F.col("p.status").isin(["SUCCESS", "FAILED"]))
    .select(F.col("p.amount"), F.col("o.discount"))
    .orderBy(F.col("amount").desc(), F.col("discount").desc())
)
