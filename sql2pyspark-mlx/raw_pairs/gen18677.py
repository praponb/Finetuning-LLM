from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date_period", "id")
w2 = Window.partitionBy("order_id").orderBy("delivered_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "carrier").orderBy("session_no", "id")
w4 = Window.orderBy(F.col("delivered_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.partitionBy("order_id", "carrier")
w6 = Window.partitionBy("order_id").orderBy(F.col("session_start_flag").asc_nulls_first())

valid = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-06-26").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("Flash"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.date_trunc("WEEK", "delivered_date").alias("delivered_date_period"),
        F.date_sub("delivered_date", 6).alias("delivered_date_minus_6d"),
    )
)

gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.col("delivered_date_period") - F.lag("delivered_date_period").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

deduped = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "delivered_date_period",
        "delivered_date_minus_6d",
        "session_start_flag",
        "session_no",
    )
)

kept_keys = (
    deduped
    .select("carrier", "order_id")
    .intersect(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("carrier", "order_id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .filter(F.col("carrier").isin(["Kerry", "DHL", "Flash"]))
    .select(
        "carrier",
        "cost",
        "session_start_flag",
        F.sum("session_no").over(w4).alias("session_no_sum_90d"),
        F.max("session_no").over(w5).alias("order_id_max_session_no"),
        F.percent_rank().over(w6).alias("session_start_flag_percent_rank"),
    )
)

valid_shipments = (
    with_windows
    .filter(~F.col("carrier").eqNullSafe(F.lit("UPS")))
    .select(
        "carrier",
        "cost",
        "session_no_sum_90d",
        "session_start_flag_percent_rank",
        F.when(F.col("cost") >= 73, "High").when(F.col("cost") >= 17, "Medium").otherwise("Low").alias("cost_band"),
        F.length("carrier").alias("carrier_length"),
    )
)

base = (
    valid_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "carrier",
        "cost",
        "session_start_flag_percent_rank",
        "cost_band",
        "carrier_length",
        F.round((F.col("carrier_length") * 1.07), 2).alias("carrier_length_adjusted"),
        (F.col("session_no_sum_90d") / F.nullif("session_start_flag_percent_rank", F.lit(0))).alias("session_no_sum_90d_per_session_start_flag_percent_rank"),
    )
)

summary = (
    base
    .groupBy(
        F.when(F.col("carrier_length") >= 81, "HIGH").otherwise("LOW").alias("carrier_length_tier"),
    )
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.min("carrier_length_adjusted").alias("min_carrier_length_adjusted"),
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"),
    )
    .filter(F.col("avg_cost") < 1000000)
)

result = (
    summary
    .select(
        "carrier_length_tier",
        "avg_cost",
        "min_carrier_length_adjusted",
        "kerry_count",
    )
    .orderBy(F.col("avg_cost").desc_nulls_first())
)
