from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("running_min_sqft", "id")
w3 = Window.orderBy("next_running_min_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("next_running_min_sqft", "id")
w5 = Window.orderBy("prev_running_min_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_max_sqft", "id")
w7 = Window.orderBy(F.col("running_avg_sqft").desc_nulls_first())

with_windows = (
    df_stores
    .filter(~F.col("region").isin(["East", "North"]))
    .select(
        "id",
        "store_name",
        "sqft",
        F.avg("sqft").over(w).alias("running_avg_sqft"),
        F.min("sqft").over(w).alias("running_min_sqft"),
        F.max("sqft").over(w).alias("running_max_sqft"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "store_name",
        "sqft",
        "running_avg_sqft",
        "running_max_sqft",
        F.lag("running_min_sqft").over(w2).alias("prev_running_min_sqft"),
        F.lead("running_min_sqft").over(w2).alias("next_running_min_sqft"),
        F.lead("running_max_sqft").over(w2).alias("next_running_max_sqft"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

metrics = (
    scored
    .withColumn("cumulative_next_running_max_sqft", F.sum("next_running_max_sqft").over(w3))
    .withColumn("next_running_min_sqft_bucket", F.ntile(4).over(w_recent))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.discount").isNotNull()), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.shipping_fee") >= 22), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "sqft",
        "running_max_sqft",
        "prev_running_min_sqft",
        "next_running_min_sqft",
        "next_running_max_sqft",
        "seq_no",
        "cumulative_next_running_max_sqft",
        F.sum("sqft").over(w5).alias("running_sqft"),
        F.lag("next_running_min_sqft", 2).over(w6).alias("prev_next_running_min_sqft"),
        F.dense_rank().over(w7).alias("running_avg_sqft_dense_rank"),
    )
)
