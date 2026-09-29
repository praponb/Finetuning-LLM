from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.filter(F.col("country").isin(["TH", "JP", "SG"])).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .filter(F.col("c.signup_date") < "2023-08-22")
    .select(F.col("c.id"), F.col("c.segment"))
    .orderBy("id")
)
