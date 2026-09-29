from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.method") == "bank_transfer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bank_transfer"),
    )
    .orderBy("status", F.col("pct_bank_transfer").desc())
)
