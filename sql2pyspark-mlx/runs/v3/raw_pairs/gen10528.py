from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("order_date") < "2025-09-01").alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_semi")
    .filter(F.col("c.signup_date") > "2023-11-02")
    .select(F.col("c.id"), F.col("c.city"), F.col("c.age"))
    .orderBy("age", "id")
    .limit(5)
)
