from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["EXPIRED", "CANCELLED"]) | (F.col("end_date") <= "2025-05-01"))
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(F.sort_array(F.collect_set("plan")).alias("plan_list"))
)
