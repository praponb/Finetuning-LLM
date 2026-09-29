from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_sessions
    .select("id", "device", "browser", "country", "new_group_flag")
    .orderBy(F.col("new_group_flag").desc_nulls_last())
)
