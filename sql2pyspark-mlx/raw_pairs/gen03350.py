from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device", "browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.row_number().over(w).alias("event_seq"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country"),
        F.last("country", ignorenulls=True).over(w3).alias("last_known_country_2"),
    )
)
