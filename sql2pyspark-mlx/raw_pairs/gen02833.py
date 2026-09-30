from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

deduped = (
    df_sessions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("week", "started_at").alias("week_start"))
    .agg(
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_rows"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
    )
    .filter(F.col("edge_rows") < 1000)
    .orderBy(F.col("edge_rows").asc_nulls_last())
)
