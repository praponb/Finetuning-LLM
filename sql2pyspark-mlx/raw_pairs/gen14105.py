from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id")
w2 = Window.partitionBy(F.month("shipped_date"), "carrier").orderBy("delivered_date", "id")
w3 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("delivered_date", "id")
w_recent = Window.partitionBy("status", "order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_shipments
    .filter(F.col("order_id") == 35)
    .select(
        "*",
        F.lag("cost", 1).over(w).alias("prev_cost"),
        F.row_number().over(w2).alias("event_seq"),
        F.first("carrier", ignorenulls=True).over(w3).alias("first_known_carrier"),
        F.lag("cost", 1).over(w4).alias("prev_cost_2"),
        F.sum("cost").over(w_recent).alias("cumulative_cost"),
    )
)

result = (
    metrics
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        "prev_cost",
        "event_seq",
        "first_known_carrier",
        "prev_cost_2",
    )
    .orderBy(F.col("cost").asc_nulls_last(), F.col("prev_cost_2").desc())
)
