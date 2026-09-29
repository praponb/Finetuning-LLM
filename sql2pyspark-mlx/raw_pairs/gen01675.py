from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(-3, Window.currentRow)
w_recent = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("carrier").orderBy("row_seq", "id")

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("shipped_date") <= "2023-03-01"))
    .select(
        "*",
        F.round("cost", 2).alias("cost_rounded"),
        F.date_trunc("month", "delivered_date").alias("delivered_date_period"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.store_id").isin([23, 25, 34]))
    .select(
        "s.id",
        "s.order_id",
        "s.carrier",
        "s.delivered_date",
        "s.cost",
        "s.cost_rounded",
        "s.carrier_filled",
        F.col("p.status").alias("order_status"),
    )
)

enriched = (
    joined
    .withColumn("last4_avg_cost_rounded", F.avg("cost_rounded").over(w_grp))
    .withColumn("last4_max_cost_rounded", F.max("cost_rounded").over(w_grp))
    .withColumn("last4_count_cost_rounded", F.count("cost_rounded").over(w_grp))
    .withColumn("last4_min_cost_rounded", F.min("cost_rounded").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_recent))
)

base = (
    enriched
    .filter(F.col("order_id").isNotNull() & (F.col("delivered_date") > F.to_date(F.lit("2023-06-20"))))
    .select(
        "*",
        F.when(F.col("last4_avg_cost_rounded") >= 66, "High").when(F.col("last4_avg_cost_rounded") >= 38, "Medium").otherwise("Low").alias("last4_avg_cost_rounded_band"),
        F.concat_ws("-", "carrier", "order_status").alias("carrier_order_status_key"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

with_dims = (
    base
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id").eqNullSafe(F.col("p.id")), "left")
    .select("src.*", F.col("p.order_date"), F.col("p.status"))
)

deduped = (
    with_dims
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "carrier",
        "cost_rounded",
        "carrier_filled",
        "order_status",
        "last4_max_cost_rounded",
        "last4_count_cost_rounded",
        "row_seq",
        "carrier_order_status_key",
        "resolved_carrier",
        "order_date",
    )
)

result = (
    deduped
    .select(
        "id",
        "carrier",
        "carrier_filled",
        "order_status",
        "last4_max_cost_rounded",
        "last4_count_cost_rounded",
        "row_seq",
        "carrier_order_status_key",
        "resolved_carrier",
    )
    .orderBy(F.col("carrier").desc_nulls_last(), "row_seq")
)
