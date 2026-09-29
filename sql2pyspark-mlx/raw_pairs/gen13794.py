from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.hour(F.col("p.paid_at")).between(9, 17))
    .groupBy(
        F.when(F.year(F.col("s.opened_date")) < 2024, "before_2024").when(F.year(F.col("s.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.sum(F.col("p.amount")).alias("total_amount"),
        F.sum(F.when(F.col("p.status") == "REFUNDED", F.col("p.amount")).otherwise(0)).alias("refunded_amount"),
    )
)
