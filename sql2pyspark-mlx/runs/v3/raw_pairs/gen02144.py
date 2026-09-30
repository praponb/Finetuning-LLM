from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.email").isNull())
    .select(
        F.col("c.segment"),
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
    )
    .orderBy(F.col("credit_limit").desc())
)
