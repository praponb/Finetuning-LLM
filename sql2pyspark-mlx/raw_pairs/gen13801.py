from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page").orderBy(F.col("duration_sec").desc())
w2 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

base = (
    df_web_events
    .filter(~F.col("page").eqNullSafe(F.lit("/product")))
    .select(
        "*",
        F.col("duration_sec").cast("string").alias("duration_sec_num"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

result = (
    base
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec_num",
        F.rank().over(w).alias("duration_sec_rank"),
        F.sum("duration_sec").over(w2).alias("duration_sec_sum_7d"),
    )
)
