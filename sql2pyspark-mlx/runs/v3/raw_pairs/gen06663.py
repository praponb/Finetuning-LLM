from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(~F.col("city").isin(["Tokyo", "Berlin", "Singapore"])).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.plan") == "basic")
    .select(F.col("s.id"), F.col("s.status"))
    .orderBy(F.col("id").desc())
    .limit(5)
)
