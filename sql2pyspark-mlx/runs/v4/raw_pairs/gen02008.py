from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("s.device").isin(["tablet", "mobile", "desktop"]))
    .select(
        F.col("c.age"),
        F.col("c.name"),
        F.col("c.email"),
        F.col("s.country"),
        F.col("s.browser"),
        F.upper(F.trim(F.col("c.name"))).alias("name_normalized"),
    )
)
