from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id")

deduped = (
    df_web_events
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "page")
)

result = (
    deduped
    .groupBy("page")
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_rows"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_rows"),
    )
    .orderBy(F.col("home_rows").desc())
)
