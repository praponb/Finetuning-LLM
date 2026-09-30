from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy("shipped_date", "id")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy(F.month("delivered_date"), "order_id").orderBy("shipped_date", "id")
w4 = Window.partitionBy("carrier", "status").orderBy(F.col("cost").desc_nulls_last())
w5 = Window.partitionBy("status").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.lag("cost", 1).over(w).alias("prev_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.lead("cost", 2, 0).over(w3).alias("next_cost"),
        F.dense_rank().over(w4).alias("cost_dense_rank"),
        F.row_number().over(w5).alias("event_seq"),
    )
)
