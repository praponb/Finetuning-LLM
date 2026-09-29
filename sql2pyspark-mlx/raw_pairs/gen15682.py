from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "started_at"), "device").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("device") == "tablet")
    .select(
        "id",
        "started_at",
        "device",
        F.row_number().over(w).alias("started_at_row"),
    )
)
