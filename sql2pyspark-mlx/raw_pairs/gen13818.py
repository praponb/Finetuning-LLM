from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "status",
        F.avg("cost").over(w).alias("running_avg_cost"),
        F.max("cost").over(w).alias("running_max_cost"),
        F.min("cost").over(w).alias("running_min_cost"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
