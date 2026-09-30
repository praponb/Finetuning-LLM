from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "channel").orderBy("order_date", "id")

ranked = (
    df_orders
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .groupBy(F.date_trunc("week", "order_date").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
    )
)
