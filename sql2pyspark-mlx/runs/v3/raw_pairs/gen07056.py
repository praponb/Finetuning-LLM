from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter((F.col("s.browser") == "Edge") | F.col("s.browser").isin(["Safari", "Chrome", "Edge"]))
    .select(
        F.col("s.browser"),
        F.col("s.country"),
        F.col("c.is_active"),
        F.col("c.name"),
    )
    .orderBy("browser", "name")
)
