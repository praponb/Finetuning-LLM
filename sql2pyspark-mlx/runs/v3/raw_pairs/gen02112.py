from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") > "2025-07-05") | (F.col("end_date") <= "2023-04-24"))
    .groupBy("customer_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"))
    .orderBy("plan_values", F.col("customer_id").desc())
)
