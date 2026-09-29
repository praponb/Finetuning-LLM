from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.to_date(F.col("p.paid_at")) < "2023-12-01")
    .groupBy(
        F.when(F.col("o.discount") >= 0.38, "high").when(F.col("o.discount") >= 0.17, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.status")))).alias("status_values"),
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
    )
)
