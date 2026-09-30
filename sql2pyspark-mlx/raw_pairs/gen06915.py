from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.year("end_date").alias("end_year"),
        F.date_trunc("MONTH", "start_date").alias("start_month"),
    )
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
    .orderBy(F.col("end_year").desc(), "start_month")
    .limit(3)
)
