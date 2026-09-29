from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy(F.to_date("started_at")).orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.row_number().over(w_part).alias("seq_no"),
        F.last("browser", ignorenulls=True).over(w2).alias("last_known_browser"),
    )
)
