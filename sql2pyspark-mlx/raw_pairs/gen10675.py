from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("opened_date", "id")
w_running = Window.partitionBy("city").orderBy("opened_date", "id")
w3 = Window.orderBy("sqft", "id")
w4 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w5 = Window.orderBy(F.col("sqft").desc(), "id")

scored = (
    df_stores
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.lag("sqft", 1, 0).over(w_hist).alias("prev_sqft"),
        F.row_number().over(w_running).alias("row_seq"),
        F.lead("sqft", 1).over(w3).alias("next_sqft"),
        F.ntile(10).over(w4).alias("sqft_bucket"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id") == 5), "left_semi")
)

with_windows = (
    kept
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("sqft_bucket_2", F.ntile(5).over(w5))
    .withColumn("event_seq", F.row_number().over(w5))
)

kept_stores = (
    with_windows
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.amount").between(1180, 1260)), "left_semi")
)

result = (
    kept_stores
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date"))
    .groupingSets(
        [["seq_no", "activity_month"], ["seq_no"], []],
        "seq_no",
        "activity_month",
    )
    .agg(
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.round(F.avg("sqft_bucket_2"), 2).alias("avg_sqft_bucket_2"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.grouping("seq_no").alias("gid"),
    )
    .filter(F.col("sqft_bit_and") > 1)
    .orderBy(F.col("sqft_bit_and").asc_nulls_first())
)
