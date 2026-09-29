from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy("delivered_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("delivered_date_plus_months").desc_nulls_last())
w5 = Window.partitionBy("order_id").orderBy("delivered_date_plus_months", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("status")
w7 = Window.partitionBy("carrier")

valid = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-10-05"))) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.add_months("delivered_date", 1).alias("delivered_date_plus_months"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
    )
)

metrics = (
    valid
    .withColumn("cost_bucket", F.ntile(3).over(w))
    .withColumn("cost_cume_dist", F.cume_dist().over(w2))
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w3))
)

latest = (
    metrics
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

metrics_shipments = (
    latest
    .select(
        "*",
        F.avg("cost_cume_dist").over(w5).alias("cost_cume_dist_avg_6"),
        F.round((F.col("cost") / F.sum("cost").over(w6)), 4).alias("cost_share"),
        F.round((F.col("last_known_cost") / F.sum("last_known_cost").over(w7)), 4).alias("last_known_cost_share"),
    )
)

enriched = (
    metrics_shipments
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.discount"), F.col("b.order_date"))
)

matched_keys = (
    enriched
    .select("order_id", "status")
    .intersect(
        df_shipments
        .filter(F.col("shipped_date") < "2023-04-23")
        .select("order_id", "status")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("carrier").isin(["DHL", "FedEx", "Flash"]))
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "delivered_date_plus_months",
        "carrier_filled",
        "cost_bucket",
        "cost_cume_dist",
        "last_known_cost",
        "rank_in_group",
        "cost_cume_dist_avg_6",
        "cost_share",
        "last_known_cost_share",
        "discount",
        F.col("discount").cast("int").alias("discount_int"),
        (F.col("discount") + F.col("cost_share")).alias("discount_plus_cost_share"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("resolved_carrier"),
    )
)

result = (
    filtered
    .select(
        "id",
        "carrier",
        "cost",
        "delivered_date_plus_months",
        "carrier_filled",
        "cost_bucket",
        "cost_cume_dist",
        "last_known_cost",
        "rank_in_group",
    )
    .orderBy(F.col("delivered_date_plus_months").desc(), F.col("id").desc())
)
