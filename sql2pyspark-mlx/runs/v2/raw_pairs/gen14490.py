from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("o.status").isin(["COMPLETED", "SHIPPED", "CANCELLED"]))
    .groupBy(F.col("o.status"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
    )
    .orderBy(F.col("status").desc())
)
