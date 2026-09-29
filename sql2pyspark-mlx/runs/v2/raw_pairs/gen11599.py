from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 1520) & F.hour("paid_at").between(6, 10) & (F.col("status") != "REFUNDED"))
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
    )
    .orderBy(F.col("failed_count").desc(), F.col("order_id").desc())
)
