from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost")
w2 = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")
w3 = Window.partitionBy("carrier")
w_running = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .select(
        "*",
        F.percent_rank().over(w).alias("cost_percent_rank"),
        F.row_number().over(w2).alias("seq_no"),
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share"),
        F.first("cost", ignorenulls=True).over(w_running).alias("first_known_cost"),
        F.row_number().over(w5).alias("event_seq"),
    )
)
