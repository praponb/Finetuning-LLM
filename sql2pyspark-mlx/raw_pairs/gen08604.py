from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status").orderBy("shipped_date", "id")

with_windows = (
    df_shipments
    .filter(F.col("cost").between(73, 90))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.lead("cost").over(w_running).alias("next_cost"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

result = (
    with_windows
    .select("id", "carrier", "delivered_date", "cost", "next_cost")
    .orderBy("cost")
)
