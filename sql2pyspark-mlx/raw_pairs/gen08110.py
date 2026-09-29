from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("delivered_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.avg("cost").over(w).alias("last7_avg_cost"),
        F.max("cost").over(w).alias("last7_max_cost"),
        F.sum("cost").over(w).alias("last7_sum_cost"),
    )
)
