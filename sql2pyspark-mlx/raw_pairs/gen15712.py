from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("level").orderBy("num_context", "id")
w2 = Window.partitionBy("host", "level").orderBy("num_context", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.size("context").alias("num_context"),
    )
)

result = (
    parsed
    .withColumn("next_status_code", F.lead("status_code").over(w_grp))
    .withColumn("prev_num_context", F.lag("num_context").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("first_known_num_context", F.first("num_context", ignorenulls=True).over(w2))
)
