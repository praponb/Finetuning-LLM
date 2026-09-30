from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.year("end_date") >= 2024) & F.month("start_date").isin([2, 5, 12]) & F.month("start_date").isin([8, 11, 12]))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.min("end_date").alias("first_end_date"),
    )
    .orderBy("first_end_date", "status_values")
)
