from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("browser").isNull())
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.row_number().over(w).alias("row_seq"),
        F.first("browser", ignorenulls=True).over(w2).alias("first_known_browser"),
        F.row_number().over(w3).alias("event_seq"),
    )
)
