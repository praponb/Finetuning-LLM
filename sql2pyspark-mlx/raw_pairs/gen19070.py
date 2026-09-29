from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost_rounded").desc_nulls_first())
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w3 = Window.partitionBy(F.month("delivered_date"), "status").orderBy("cost_rounded", "id")

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & F.month("shipped_date").isin([3, 8, 12]))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
        F.round("cost", 2).alias("cost_rounded"),
    )
)

result = (
    valid
    .filter(F.col("order_id") == 27)
    .select(
        "id",
        "order_id",
        "shipped_date",
        F.cume_dist().over(w).alias("cost_rounded_cume_dist"),
        F.lag("cost_rounded", 1).over(w2).alias("prev_cost_rounded"),
        F.ntile(3).over(w3).alias("cost_rounded_bucket"),
    )
)
