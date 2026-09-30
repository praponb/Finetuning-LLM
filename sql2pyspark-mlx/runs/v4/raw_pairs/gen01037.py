from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("seq_no", "id")
w4 = Window.orderBy("shipping_fee")
w5 = Window.partitionBy("customer_id").orderBy("row_seq", "id")
w6 = Window.partitionBy("customer_id", "channel").orderBy(F.col("amount").desc(), "id")
w7 = Window.partitionBy("store_id", "channel")
w8 = Window.partitionBy("channel")
w9 = Window.partitionBy("customer_id").orderBy("prev_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_orders
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.lead("seq_no", 2).over(w3).alias("next_seq_no"),
        F.rank().over(w4).alias("shipping_fee_rank"),
        F.lag("seq_no", 2).over(w5).alias("prev_seq_no"),
        F.ntile(10).over(w6).alias("amount_bucket"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w7)), 4).alias("row_seq_share"),
    )
)

scored = (
    with_windows
    .withColumn("amount_bucket_share", F.round((F.col("amount_bucket") / F.sum("amount_bucket").over(w8)), 4))
    .withColumn("cumulative_next_seq_no", F.sum("next_seq_no").over(w9))
)

matched_keys = (
    scored
    .select("customer_id", "store_id")
    .intersect(
        df_orders
        .filter(F.col("shipping_fee").between(11, 23))
        .select("customer_id", "store_id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("seq_no")
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.max(F.when(F.col("status") == "COMPLETED", F.col("seq_no"))).alias("completed_max_seq_no"),
    )
    .orderBy(F.col("num_channels").desc())
)
