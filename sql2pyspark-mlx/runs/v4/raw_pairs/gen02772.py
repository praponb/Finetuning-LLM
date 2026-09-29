from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_last())
w_hist = Window.partitionBy("carrier").orderBy("cost", "id")
w3 = Window.partitionBy("carrier", "status").orderBy("delivered_date", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("carrier", "order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("carrier").orderBy(F.col("last_known_cost").asc_nulls_first(), "id")
w7 = Window.partitionBy("status", "carrier")
w8 = Window.orderBy(F.col("last_known_cost").desc(), "id")
w9 = Window.orderBy("cost", "id")
w10 = Window.partitionBy("carrier", "order_id").orderBy("rank_in_group", "id").rowsBetween(-6, -1)
w11 = Window.partitionBy("status")
w12 = Window.partitionBy("carrier", "status").orderBy("shipping_fee", "id")

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.lower("carrier").alias("carrier_lower"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.rank().over(w).alias("cost_rank"),
        F.ntile(3).over(w_hist).alias("cost_bucket"),
        F.avg("cost").over(w3).alias("cost_avg_6"),
        F.last("cost", ignorenulls=True).over(w4).alias("last_known_cost"),
        F.sum("cost").over(w_grp).alias("running_cost"),
    )
)

latest = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    latest
    .withColumn("status_min_cost", F.min("cost").over(w7))
    .withColumn("row_seq", F.row_number().over(w8))
    .withColumn("cost_bucket_2", F.ntile(5).over(w9))
)

scored = (
    with_windows
    .select(
        "*",
        F.avg("running_cost").over(w10).alias("running_cost_avg_6_prior"),
        F.round((F.col("running_cost") / F.sum("running_cost").over(w11)), 4).alias("running_cost_share"),
    )
)

enriched = (
    scored
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.channel"), F.col("p.shipping_fee"))
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w12))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "carrier_lower",
        "carrier_clean",
        "cost_rank",
        F.when(F.col("carrier").rlike("^(Kerry|DHL)$"), "EXTENDED").when(F.col("carrier").rlike("^(FedEx|Flash)$"), "GROUP_B").otherwise("GROUP_A").alias("carrier_zone"),
    )
    .orderBy(F.col("status").desc_nulls_first())
)
