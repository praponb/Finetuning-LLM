from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.filter(F.col("country").isin(["UK", "DE"])).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .select(F.col("c.id"), F.col("c.email"))
    .orderBy(F.col("id").desc(), F.col("email").desc())
)
