from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy(F.month("started_at"), "customer_id").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.row_number().over(w_ordered).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.first("browser", ignorenulls=True).over(w3).alias("first_known_browser"),
        F.row_number().over(w4).alias("row_seq_2"),
    )
)
