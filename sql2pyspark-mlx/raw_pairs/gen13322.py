from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("is_ups", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("is_ups").desc_nulls_last())
w3 = Window.partitionBy("status").orderBy("cost", "id")
w4 = Window.orderBy("is_ups", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("status").orderBy(F.col("next_is_ups").desc_nulls_first())
w6 = Window.partitionBy("status").orderBy(F.col("cost_minus_cost_avg_2_prior").desc_nulls_last())
w7 = Window.partitionBy("status")
w8 = Window.partitionBy("status_max_next_is_ups_rank_tier").orderBy(F.col("num_status").desc())

base = (
    df_shipments
    .filter((~F.col("carrier").eqNullSafe(F.lit("FedEx"))) & (F.col("status") != "RETURNED"))
    .select(
        "*",
        F.when(F.col("carrier") == "UPS", 1).otherwise(0).alias("is_ups"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled_2"),
    )
)

enriched = (
    base
    .filter(F.col("status").isin(["DELIVERED", "LOST", "IN_TRANSIT"]))
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "carrier_filled",
        "carrier_filled_2",
        F.last("cost", ignorenulls=True).over(w).alias("last_known_cost"),
        F.dense_rank().over(w2).alias("is_ups_dense_rank"),
        F.lead("is_ups", 2).over(w3).alias("next_is_ups"),
        F.avg("cost").over(w4).alias("cost_avg_2_prior"),
    )
)

kept_keys = (
    enriched
    .select("status", "id")
    .subtract(
        df_shipments
        .filter(F.month("delivered_date").isin([1, 10, 11]))
        .select("status", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys = (
    reconciled
    .select("id")
    .exceptAll(
        df_shipments
        .filter(F.col("delivered_date").between("2024-05-20", "2025-05-11"))
        .select("id")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("shipped_date") > "2024-05-01")
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "carrier_filled",
        "carrier_filled_2",
        "last_known_cost",
        "is_ups_dense_rank",
        "next_is_ups",
        (F.col("cost") - F.col("cost_avg_2_prior")).alias("cost_minus_cost_avg_2_prior"),
    )
)

with_windows = (
    valid
    .withColumn("next_is_ups_rank", F.rank().over(w5))
    .withColumn("cost_minus_cost_avg_2_prior_cume_dist", F.cume_dist().over(w6))
)

with_windows_shipments = (
    with_windows
    .select(
        "shipped_date",
        "delivered_date",
        "status",
        "carrier_filled",
        "carrier_filled_2",
        "last_known_cost",
        "is_ups_dense_rank",
        "cost_minus_cost_avg_2_prior",
        "cost_minus_cost_avg_2_prior_cume_dist",
        F.max("next_is_ups_rank").over(w7).alias("status_max_next_is_ups_rank"),
        F.max("is_ups_dense_rank").over(w7).alias("status_max_is_ups_dense_rank"),
    )
)

result = (
    with_windows_shipments
    .groupBy(
        F.when(F.col("status_max_next_is_ups_rank") >= 7, "LARGE").otherwise("SMALL").alias("status_max_next_is_ups_rank_tier"),
        "status",
    )
    .agg(
        F.percentile_approx("status_max_is_ups_dense_rank", [0.25, 0.5, 0.75]).alias("status_max_is_ups_dense_rank_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("num_status") > 1)
    .withColumn("num_status_percent_rank", F.percent_rank().over(w8))
    .orderBy(F.col("num_status").desc_nulls_last())
)
