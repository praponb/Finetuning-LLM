from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "started_at")).orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("country", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w4 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "desktop", "mobile"]))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        F.row_number().over(w).alias("seq_no"),
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
        F.row_number().over(w3).alias("row_seq"),
        F.first("browser", ignorenulls=True).over(w4).alias("first_known_browser"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "browser",
        "started_at",
        "seq_no",
        "first_known_country",
        "row_seq",
        "first_known_browser",
    )
)
