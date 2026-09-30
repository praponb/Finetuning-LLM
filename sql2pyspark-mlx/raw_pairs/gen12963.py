from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("country", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("WEEK", "started_at"), "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select(
        "*",
        F.first("browser", ignorenulls=True).over(w_ordered).alias("first_known_browser"),
        F.last("browser", ignorenulls=True).over(w2).alias("last_known_browser"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
