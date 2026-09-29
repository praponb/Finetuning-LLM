from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id")

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & (~F.col("page").eqNullSafe(F.lit("/cart"))))
    .select(
        "*",
        F.when(F.col("duration_sec") >= 419, "High").when(F.col("duration_sec") >= 99, "Medium").otherwise("Low").alias("duration_sec_tier"),
    )
)

result = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
