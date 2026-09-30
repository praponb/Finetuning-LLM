from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device", "browser").orderBy("started_at", "id")
w_grp_2 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")

metrics = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.first("browser", ignorenulls=True).over(w_grp).alias("first_known_browser"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country"),
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w_grp_2).alias("seq_no"),
    )
)

result = metrics.select("id", "device", "browser", "first_known_browser", "seq_no")
