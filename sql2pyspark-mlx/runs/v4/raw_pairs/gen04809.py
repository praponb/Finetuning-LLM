from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy(F.col("shipped_date").desc_nulls_last())
w_part = Window.partitionBy("order_status").orderBy("cost_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("cost_filled", "id")

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .withColumn("cost_whole", F.ceil("cost"))
    .withColumn("carrier_status", F.concat("carrier", F.lit("/"), "status"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("N/A")))
)

with_dims = (
    valid
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.order_date"), F.col("d.status").alias("order_status"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.shipping_fee"), F.col("b.discount"), F.col("b.amount"))
)

base = (
    joined
    .filter(F.col("cost_whole") > 48)
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "carrier_filled",
        "order_status",
        "amount",
        F.when(F.col("cost") >= 95, "High").when(F.col("cost") >= 18, "Medium").otherwise("Low").alias("cost_band"),
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

deduped = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.sum("amount").over(w_part).alias("cumulative_amount"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

matched_keys = (
    enriched
    .select("carrier")
    .subtract(
        df_shipments
        .filter(F.col("carrier").isin(["Kerry", "FedEx", "UPS"]))
        .select("carrier")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

grouped = (
    reconciled
    .withColumn("row_seq_tier", F.when(F.col("row_seq") >= 9, "LARGE").otherwise("SMALL"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 1430, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["row_seq_tier", "amount_tier"], ["row_seq_tier"], []],
        "row_seq_tier",
        "amount_tier",
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("carrier") == "Flash", 1)), F.lit(0)), 2).alias("amount_per_flash"),
        F.percentile_approx("cumulative_amount", [0.25, 0.5, 0.75]).alias("cumulative_amount_quartiles"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("order_status") == "RETURNED", 1)), F.lit(0)), 2).alias("row_seq_per_returned"),
        F.sum(F.when(F.col("order_status") == "RETURNED", F.col("cumulative_amount")).otherwise(0)).alias("returned_cumulative_amount"),
    )
)

result = (
    grouped
    .select(
        "row_seq_tier",
        "amount_tier",
        "status_list",
        "amount_per_flash",
        "cumulative_amount_quartiles",
    )
    .orderBy(F.col("amount_per_flash").desc_nulls_first(), F.col("row_seq_tier").desc())
)
