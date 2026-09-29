from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("WEEK", "started_at"), "country").orderBy("started_at", "id")
w3 = Window.partitionBy(F.to_date("started_at"), "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .filter(F.col("customer_id").isin([4, 7, 18, 35]))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
        F.row_number().over(w2).alias("event_seq"),
        F.first("browser", ignorenulls=True).over(w3).alias("first_known_browser"),
    )
)
