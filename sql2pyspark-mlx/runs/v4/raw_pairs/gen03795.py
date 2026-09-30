from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([2, 3, 7]))
    .groupBy(
        "status",
        F.date_format("start_date", "yyyy-MM").alias("start_month"),
        F.date_trunc("MONTH", "end_date").alias("end_month"),
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"))
)
