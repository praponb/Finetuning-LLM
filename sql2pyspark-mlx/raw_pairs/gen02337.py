from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("service").orderBy("status_code", "id")
w2 = Window.partitionBy("host").orderBy(F.col("num_context").desc(), "id")

flattened_attrs = (
    df_app_logs
    .filter(F.col("status_code").between(427, 441))
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "context",
        F.col("context")["version"].alias("version"),
        F.size("context").alias("num_context"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "service",
        "version",
        F.lead("num_context").over(w_recent).alias("next_num_context"),
        F.lead("status_code").over(w_recent).alias("next_status_code"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
