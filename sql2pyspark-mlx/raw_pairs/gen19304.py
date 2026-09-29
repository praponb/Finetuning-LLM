from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.length("name") >= 11).select("id")

result = (
    df_sessions.alias("s")
    .join(excluded_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("s.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .filter(F.col("s.customer_id").isNotNull())
    .select(F.col("s.id"), F.col("s.country"))
    .orderBy(F.col("country").desc())
)
