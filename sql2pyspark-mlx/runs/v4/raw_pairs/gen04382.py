from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_last())
w_part = Window.partitionBy("city", "region").orderBy("revenue", "id")
w4 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc())
w5 = Window.partitionBy("region").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("region").orderBy("sqft_cume_dist", "id")
w7 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w8 = Window.orderBy("rn", "id")
w9 = Window.partitionBy("city", "region").orderBy(F.col("row_seq").desc())
w10 = Window.orderBy("sqft_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("running_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("city").orderBy(F.col("rn").desc(), "id")
w13 = Window.partitionBy("region")

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-08-27").cast("date")) & (F.col("opened_date") < "2024-06-18"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
        F.trim("region").alias("clean_region"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"))
)

metrics = (
    with_latest
    .withColumn("sqft_cume_dist", F.cume_dist().over(w2))
    .withColumn("row_seq", F.row_number().over(w_part))
)

deduped = (
    metrics
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.sum("revenue").over(w5).alias("running_revenue"),
        F.lag("sqft", 1, 0).over(w6).alias("prev_sqft"),
        F.row_number().over(w7).alias("event_seq"),
        F.lead("sqft_cume_dist", 2, 0).over(w8).alias("next_sqft_cume_dist"),
        F.cume_dist().over(w9).alias("row_seq_cume_dist"),
    )
)

metrics_stores = (
    with_windows
    .select(
        "*",
        F.lag("sqft_cume_dist", 1).over(w6).alias("prev_sqft_cume_dist"),
        F.sum("next_sqft_cume_dist").over(w10).alias("running_next_sqft_cume_dist"),
        F.sum("sqft").over(w11).alias("cumulative_sqft"),
        F.row_number().over(w12).alias("row_seq_2"),
        F.stddev_samp("row_seq_cume_dist").over(w13).alias("region_stddev_samp_row_seq_cume_dist"),
    )
)

eligible = (
    metrics_stores
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.opened_date")), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "opened_date_label",
        "clean_region",
        "revenue",
        "row_seq",
        "running_revenue",
    )
    .orderBy(F.col("opened_date").desc())
)
