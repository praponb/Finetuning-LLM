from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("months_since_started_at", (F.datediff(F.current_date(), "started_at") / 30))
)
