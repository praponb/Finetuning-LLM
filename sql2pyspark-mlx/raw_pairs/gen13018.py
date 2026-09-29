from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("started_at"), "device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id")

with_windows = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .select(
        "id",
        "device",
        "started_at",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    with_windows
    .select("id", "device", "started_at", "seq_no")
    .orderBy(F.col("seq_no").desc())
)
