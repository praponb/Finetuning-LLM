from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec_filled").desc_nulls_last(), "id")

base = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select("*", F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"))
)

result = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)
