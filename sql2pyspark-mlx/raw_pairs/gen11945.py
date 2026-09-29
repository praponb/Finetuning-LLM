from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.orderBy("started_at", "id")
w3 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .select(
        "id",
        "started_at",
        "country",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("country", ignorenulls=True).over(w3).alias("last_known_country"),
    )
)
