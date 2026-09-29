from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-04-28", "2024-11-01") & (F.col("start_date").between("2023-08-01", "2025-02-01") | (F.col("plan") == "standard")))
    .groupBy(F.year("start_date").alias("start_year"))
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"))
    .orderBy("plan_values")
)
