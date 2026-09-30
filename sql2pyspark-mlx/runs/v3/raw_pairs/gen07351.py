from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.filter(F.col("country") == "TH").alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .filter(~F.col("c.country").isin(["UK", "JP"]))
    .select(F.col("c.id"), F.col("c.age"))
)
