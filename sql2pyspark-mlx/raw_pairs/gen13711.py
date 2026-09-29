from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country"),
    )
)
