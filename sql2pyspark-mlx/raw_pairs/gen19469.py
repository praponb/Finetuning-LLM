from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.when(F.year(F.col("o.order_date")) < 2024, "before_2024").when(F.year(F.col("o.order_date")) == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
        F.col("o.channel"),
    )
    .agg(
        F.max(F.col("p.amount")).alias("max_amount"),
        F.sum(F.when(F.col("p.status") == "SUCCESS", F.col("p.amount")).otherwise(0)).alias("success_amount"),
    )
    .filter(F.col("success_amount") > 21)
    .orderBy(F.col("max_amount").desc(), "order_cohort", "channel")
    .limit(3)
)
