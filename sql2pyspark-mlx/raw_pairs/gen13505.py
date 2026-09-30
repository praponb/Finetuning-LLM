from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status").orderBy("delivered_date_day", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy(F.year("shipped_date"), "status").orderBy("cost_scaled", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("carrier").orderBy("last4_count_cost_scaled", "id")
w4 = Window.partitionBy("carrier")
w5 = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_last(), "id")
w6 = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_first(), "id")

base = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "Flash"]))
    .select(
        "*",
        F.round((F.col("cost") * 1.07), 2).alias("cost_scaled"),
        F.dayofmonth("delivered_date").alias("delivered_date_day"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

metrics = (
    base
    .filter(F.col("cost_scaled") < 44)
    .select(
        "id",
        "carrier",
        "cost",
        "delivered_date_day",
        "resolved_carrier",
        F.max("cost_scaled").over(w_part).alias("last4_max_cost_scaled"),
        F.count("cost_scaled").over(w_part).alias("last4_count_cost_scaled"),
        F.avg("cost_scaled").over(w_part).alias("last4_avg_cost_scaled"),
        F.avg("cost").over(w2).alias("cost_avg_6_prior"),
    )
)

enriched = (
    metrics
    .withColumn("prev_last4_count_cost_scaled", F.lag("last4_count_cost_scaled", 1).over(w3))
    .withColumn("carrier_max_delivered_date_day", F.max("delivered_date_day").over(w4))
    .withColumn("last4_count_cost_scaled_share", F.round((F.col("last4_count_cost_scaled") / F.sum("last4_count_cost_scaled").over(w4)), 4))
    .withColumn("next_cost_avg_6_prior", F.lead("cost_avg_6_prior", 1).over(w3))
)

valid = (
    enriched
    .filter(F.col("next_cost_avg_6_prior").between(61, 75))
    .select(
        "*",
        F.round((F.col("prev_last4_count_cost_scaled") * 100), 2).alias("prev_last4_count_cost_scaled_adjusted"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("resolved_carrier_2"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "carrier",
        "cost",
        "last4_avg_cost_scaled",
        "prev_last4_count_cost_scaled",
    )
)

latest_shipments = (
    latest
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .select("id", "last4_avg_cost_scaled", "prev_last4_count_cost_scaled")
)

kept_keys = (
    latest_shipments
    .select("id")
    .subtract(
        df_shipments
        .filter(F.col("carrier").isin(["FedEx", "Flash", "Kerry"]))
        .select("id")
    )
)

verified = (
    latest_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("prev_last4_count_cost_scaled") >= 12, "PREMIUM").otherwise("STANDARD").alias("prev_last4_count_cost_scaled_tier"),
    )
    .agg(
        F.round(F.avg("prev_last4_count_cost_scaled"), 2).alias("avg_prev_last4_count_cost_scaled"),
        F.percentile_approx("prev_last4_count_cost_scaled", 0.5).alias("prev_last4_count_cost_scaled_p50"),
        F.percentile_approx("last4_avg_cost_scaled", [0.25, 0.5, 0.75]).alias("last4_avg_cost_scaled_quartiles"),
    )
    .filter(F.col("avg_prev_last4_count_cost_scaled") >= 0)
)
