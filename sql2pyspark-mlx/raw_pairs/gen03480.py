from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("order_id", "carrier")

with_windows = (
    df_shipments
    .select(
        "*",
        F.count("cost").over(w).alias("last3_count_cost"),
        F.avg("cost").over(w).alias("last3_avg_cost"),
        F.min("cost").over(w).alias("last3_min_cost"),
        F.sum("cost").over(w).alias("last3_sum_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "last3_count_cost",
        "last3_avg_cost",
    )
    .orderBy(F.col("cost").asc_nulls_last())
)
