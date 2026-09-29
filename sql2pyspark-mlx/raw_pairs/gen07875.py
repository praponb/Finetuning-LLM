from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "carrier")
w3 = Window.partitionBy("order_id").orderBy(F.col("cost_share").desc_nulls_last())
w_recent = Window.partitionBy("order_status").orderBy(F.col("cost_share").desc_nulls_first())
w5 = Window.orderBy(F.col("cost_share").desc_nulls_last())
w_hist = Window.partitionBy("order_id")
w7 = Window.partitionBy("carrier")

enriched = (
    df_shipments
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w2)), 4))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.channel").isin(["store", "partner"]))
    .select("e.*", F.col("p.amount"), F.col("p.status").alias("order_status"))
)

matched_keys = (
    with_dims
    .select("carrier", "status")
    .intersectAll(
        df_shipments
        .filter(F.col("shipped_date").between("2023-03-19", "2024-08-01"))
        .select("carrier", "status")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .withColumn("cost_share_rank", F.rank().over(w3))
    .withColumn("cost_share_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("cost_share_cume_dist_2", F.cume_dist().over(w5))
    .withColumn("order_id_count_amount", F.count("amount").over(w_hist))
    .withColumn("cost_share_2", F.round((F.col("cost") / F.sum("cost").over(w7)), 4))
)

result = (
    with_windows
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .groupingSets(
        [["order_status", "shipment_status"], ["order_status"]],
        F.col("p.status").alias("order_status"),
        F.col("s.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("s.cost_share_cume_dist_2")).alias("avg_cost_share_cume_dist_2"),
        F.min(F.col("s.cost")).alias("min_cost"),
        F.grouping_id().alias("gid"),
    )
)
