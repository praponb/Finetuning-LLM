from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.countDistinct("country").alias("unique_countrys"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.unique_countrys") >= 6)
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.segment"),
        F.col("ct.unique_countrys"),
    )
    .orderBy("unique_countrys", "id")
    .limit(20)
)
