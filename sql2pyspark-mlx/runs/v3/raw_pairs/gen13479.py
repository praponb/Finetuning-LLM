from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.email").like("%@mail.co.th"))
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.max(F.col("p.paid_at")).alias("last_paid_at"),
        F.count(F.col("p.amount")).alias("amount_count"),
    )
    .filter(F.col("amount_count") > 2)
    .orderBy("signup_year")
    .limit(20)
)
