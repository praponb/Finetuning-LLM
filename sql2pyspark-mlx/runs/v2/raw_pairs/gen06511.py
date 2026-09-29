from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.countDistinct("country").alias("unique_countrys"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .filter(F.col("ct.unique_countrys") >= 6)
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("c.signup_date"),
        F.col("ct.unique_countrys"),
    )
    .orderBy(F.col("age").desc())
)
