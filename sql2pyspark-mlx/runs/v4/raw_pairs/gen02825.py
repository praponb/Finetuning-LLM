from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("delivered_date", "id")
w_part = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier").orderBy("next_cost")
w5 = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_first(), "id")

with_windows = (
    df_shipments
    .select(
        "*",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.sum("cost").over(w_part).alias("running_cost"),
        F.last("carrier", ignorenulls=True).over(w3).alias("last_known_carrier"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.sum("running_cost").over(w3).alias("cumulative_running_cost"),
        F.rank().over(w4).alias("next_cost_rank"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

with_dims = (
    latest
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.shipping_fee"))
)

result = (
    with_dims
    .groupBy("carrier")
    .pivot("status", ["LOST", "RETURNED", "DELIVERED"])
    .agg(F.max("cost").alias("total"), F.count(F.lit(1)).alias("n"))
)
