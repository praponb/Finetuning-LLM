from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(F.col("segment") == "Corporate").alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.customer_id").isNotNull())
    .select(F.col("s.id"), F.col("s.device"), F.col("s.customer_id"))
    .orderBy("device")
)
