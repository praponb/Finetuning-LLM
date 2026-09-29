from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([5, 6, 10]) & (F.year("end_date") == 2023))
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count("*").alias("num_subscriptions"),
    )
)
