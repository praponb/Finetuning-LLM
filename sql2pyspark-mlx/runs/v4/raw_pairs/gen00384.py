from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("cost").asc_nulls_first())
w3 = Window.partitionBy("order_id", "status").orderBy("delivered_date", "id")
w4 = Window.partitionBy("status").orderBy("cost_rounded", "id")
w5 = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")

filtered = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & F.col("carrier").isin(["Flash", "UPS", "DHL"]))
    .select(
        "*",
        F.round("cost", 2).alias("cost_rounded"),
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.sum("cost_rounded").over(w).alias("running_cost_rounded"),
        F.cume_dist().over(w2).alias("cost_cume_dist"),
        F.lag("cost_rounded", 1, 0).over(w3).alias("prev_cost_rounded"),
        F.lead("cost_rounded", 2).over(w4).alias("next_cost_rounded"),
        F.ntile(3).over(w5).alias("cost_bucket"),
    )
)
