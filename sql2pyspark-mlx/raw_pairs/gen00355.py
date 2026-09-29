from pyspark.sql import functions as F

with_dims = (
    df_payments
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.status").alias("order_status"), F.col("p.order_date"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("amount") >= 1410, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.min("amount").alias("min_amount"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
    )
    .filter(F.col("min_amount") > 0)
    .orderBy(F.col("success_rows").desc_nulls_last())
)
