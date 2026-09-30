from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_semi")
    .filter(F.col("c.city").isin(["Singapore", "Bangkok", "New York"]))
    .select(F.col("c.id"), F.col("c.segment"), F.col("c.name"))
    .orderBy(F.col("id").desc())
)
