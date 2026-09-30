from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy("cost", "id")
w2 = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("cost").between(44, 77))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.lag("cost").over(w_recent).alias("prev_cost"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.sum("cost").over(w2).alias("running_cost"),
    )
)
