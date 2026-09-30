from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        F.row_number().over(w_grp).alias("seq_no"),
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
    )
)
