from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w2 = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_last())
w3 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status", "carrier").orderBy(F.col("delivered_date").desc_nulls_first(), "id")
w6 = Window.partitionBy("status", "order_id").orderBy("dedup_rank_times_cost", "id")
w_part = Window.partitionBy("order_id").orderBy("cost_cume_dist", "id")
w8 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "UPS"]))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w)), 4))
    .withColumn("cost_cume_dist", F.cume_dist().over(w2))
    .withColumn("last_known_carrier", F.last("carrier", ignorenulls=True).over(w3))
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w4))
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    deduped
    .filter(F.col("status") == "LOST")
    .select("*", (F.col("dedup_rank") * F.col("cost")).alias("dedup_rank_times_cost"))
)

enriched = (
    base
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.cost",
        "e.status",
        "e.cost_share",
        "e.cost_cume_dist",
        "e.last_known_carrier",
        "e.first_known_carrier",
        "e.dedup_rank",
        "e.dedup_rank_times_cost",
        F.col("o.amount"),
        F.col("o.shipping_fee"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "order_id",
        "cost",
        "cost_cume_dist",
        "last_known_carrier",
        "dedup_rank",
        "amount",
        "shipping_fee",
        F.lag("shipping_fee").over(w6).alias("prev_shipping_fee"),
        F.lead("shipping_fee").over(w6).alias("next_shipping_fee"),
        F.lead("cost_cume_dist").over(w6).alias("next_cost_cume_dist"),
        F.lag("cost_cume_dist").over(w6).alias("prev_cost_cume_dist"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "order_id",
        "cost",
        "cost_cume_dist",
        "dedup_rank",
        "amount",
        "shipping_fee",
        "next_shipping_fee",
        "prev_cost_cume_dist",
        "event_seq",
        F.lag("prev_shipping_fee").over(w_part).alias("prev_prev_shipping_fee"),
        F.lead("prev_shipping_fee").over(w_part).alias("next_prev_shipping_fee"),
        F.lead("cost").over(w_part).alias("next_cost"),
        F.lag("cost").over(w_part).alias("prev_cost"),
        F.row_number().over(w_part).alias("row_seq"),
        F.last("event_seq", ignorenulls=True).over(w8).alias("last_known_event_seq"),
    )
)
