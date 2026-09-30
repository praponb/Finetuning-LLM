from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "store_id").orderBy(F.col("discount").desc_nulls_first(), "id")
w2 = Window.partitionBy("channel").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("channel").orderBy("amount", "id")

valid = (
    df_orders
    .filter(F.col("channel").isNotNull() & (F.col("order_date") > F.to_date(F.lit("2023-08-09"))))
    .withColumn("clean_status", F.trim("status"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("UNKNOWN")))
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "channel", "amount", "resolved_channel")
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    latest
    .withColumn("prev_amount", F.lag("amount").over(w3))
    .withColumn("event_seq", F.row_number().over(w3))
)

result = (
    with_windows
    .select("id", "channel", "resolved_channel", "row_num", "prev_amount", "event_seq")
)
