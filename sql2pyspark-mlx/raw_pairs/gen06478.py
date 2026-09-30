from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.lower(F.trim(F.col("c.name"))) == "pim chaiyaporn")
    .select(F.col("c.id"), F.col("c.email"))
)
