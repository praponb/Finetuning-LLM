from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("status_code", "id", "key")
w2 = Window.partitionBy("key").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    flattened
    .select(
        "id",
        "status_code",
        "key",
        "v",
        F.lead("status_code").over(w).alias("next_status_code"),
        F.lag("status_code").over(w).alias("prev_status_code"),
        F.row_number().over(w).alias("row_seq"),
        F.last("key", ignorenulls=True).over(w2).alias("last_known_key"),
    )
)
