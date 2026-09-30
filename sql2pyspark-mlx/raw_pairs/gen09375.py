from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "customer_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("order_date", "id")
w3 = Window.partitionBy("channel").orderBy(F.col("running_avg_amount").asc_nulls_last(), "id")

filtered = (
    df_orders
    .filter(F.col("order_date") > F.to_date(F.lit("2023-08-28")))
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "amount",
        F.when(F.col("status").isin(["SHIPPED", "CANCELLED"]), "Priority").otherwise("Other").alias("status_group"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

with_windows = (
    filtered
    .filter(F.col("amount").between(1370, 1440))
    .select(
        "id",
        "order_date",
        "channel",
        "amount",
        "status_group",
        F.avg("amount").over(w).alias("running_avg_amount"),
        F.min("amount").over(w).alias("running_min_amount"),
        F.count("amount").over(w).alias("running_count_amount"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

kept = (
    ranked
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "left_semi")
)

result = (
    kept
    .withColumn("week_start", F.date_trunc("WEEK", "order_date"))
    .groupingSets([["channel", "week_start"], ["channel"]], "channel", "week_start")
    .agg(
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq_2"),
        F.grouping_id().alias("gid"),
    )
)
