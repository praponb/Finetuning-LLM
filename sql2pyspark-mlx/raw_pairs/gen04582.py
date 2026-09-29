from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("country") == "DE")
    .groupBy("customer_id")
    .agg(
        F.min("started_at").alias("first_started_at"),
        F.countDistinct("country").alias("unique_countrys"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "left")
    .filter(F.col("C.unique_countrys") > 7)
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("c.country"),
        F.col("C.first_started_at"),
        F.col("C.unique_countrys"),
    )
)
