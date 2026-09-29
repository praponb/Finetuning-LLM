from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_sessions
    .select("id", "customer_id", "started_at", "country", "session_start_flag")
    .orderBy(F.col("country").desc_nulls_last())
)
