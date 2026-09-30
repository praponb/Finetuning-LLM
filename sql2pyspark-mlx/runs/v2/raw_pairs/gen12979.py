from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(F.col("c.credit_limit"), F.col("c.city"), F.col("C.country_list"))
    .orderBy(F.col("credit_limit").desc(), F.col("city").desc())
)
