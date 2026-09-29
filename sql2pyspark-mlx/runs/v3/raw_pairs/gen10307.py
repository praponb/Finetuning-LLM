from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id") == 40)
    .groupBy(
        F.when(F.year(F.col("o.order_date")) < 2024, "before_2024").when(F.year(F.col("o.order_date")) == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.count("*").alias("payment_count"),
        F.count(F.when(F.col("p.method") == "card", 1)).alias("card_count"),
    )
    .orderBy("card_count")
)
