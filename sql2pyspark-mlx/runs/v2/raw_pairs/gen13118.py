from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "desktop", "mobile"]))
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.city"), F.col("t.country_list"))
    .orderBy(F.col("city").desc(), F.col("id").desc())
)
