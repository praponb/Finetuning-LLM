from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("num_status").desc())

result = (
    df_subscriptions
    .groupBy("customer_id", F.date_trunc("MONTH", "start_date").alias("activity_month"))
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
    )
    .withColumn("num_status_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("num_status").desc_nulls_first())
)
