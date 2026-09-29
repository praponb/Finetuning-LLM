from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc_nulls_first(), "id")

latest = (
    df_sessions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"))
    .agg(
        F.min("started_at").alias("first_started_at"),
        F.min("started_at").alias("first_seen"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
    )
)
