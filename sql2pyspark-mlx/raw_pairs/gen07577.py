from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date")
w_ordered = Window.orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy(F.date_trunc("YEAR", "opened_date")).orderBy(F.col("opened_date").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

latest = (
    df_stores
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "store_name", "opened_date")
)

kept = (
    latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (~F.col("c.status").isin(["CANCELLED", "PENDING"])), "left_semi")
)

enriched = (
    kept
    .select(
        "id",
        "store_name",
        F.row_number().over(w_ordered).alias("event_seq"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.row_number().over(w3).alias("seq_no_2"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.seq_no",
        "s.seq_no_2",
        F.col("c.discount").alias("order_discount"),
        F.col("c.shipping_fee").alias("order_shipping_fee"),
    )
)

kept_stores = (
    latest_match
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.discount").isNotNull()), "left_anti")
)

stats = (
    kept_stores
    .groupBy("seq_no")
    .agg(
        F.sum("order_shipping_fee").alias("total_order_shipping_fee"),
        F.percentile_approx("seq_no_2", 0.9).alias("seq_no_2_p90"),
        F.avg("seq_no_2").alias("avg_seq_no_2"),
        F.avg("order_discount").alias("avg_order_discount"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    stats
    .select(
        "seq_no",
        "total_order_shipping_fee",
        "seq_no_2_p90",
        "avg_order_discount",
        "num_rows",
    )
)
