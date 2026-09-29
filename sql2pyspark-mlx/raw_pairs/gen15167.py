from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w_grp = Window.partitionBy(F.year("started_at"), "country").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(~F.col("device").isin(["desktop", "tablet"]))
    .select(
        "id",
        "device",
        "browser",
        "country",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)
