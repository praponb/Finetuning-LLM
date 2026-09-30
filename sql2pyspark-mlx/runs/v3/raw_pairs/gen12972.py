from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("c.credit_limit") > 14800)
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.countDistinct(F.col("o.shipping_fee")).alias("unique_statuss"),
        F.sum(F.when(F.col("o.channel") == "partner", F.col("o.amount")).otherwise(0)).alias("partner_amount"),
    )
    .orderBy(F.col("partner_amount").desc(), "signup_year")
)
