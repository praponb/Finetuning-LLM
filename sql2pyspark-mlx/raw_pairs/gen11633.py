from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.discount") < 0.34)
    .groupBy(
        F.when(F.year(F.col("o.order_date")) < 2024, "before_2024").when(F.year(F.col("o.order_date")) == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.min(F.col("p.paid_at")).alias("first_paid_at"),
        F.max(F.col("p.paid_at")).alias("latest_paid_at"),
    )
    .orderBy(F.col("first_paid_at").desc(), "order_cohort")
    .limit(3)
)
