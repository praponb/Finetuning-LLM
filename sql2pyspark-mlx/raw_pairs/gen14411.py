from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("delivered_date"), "carrier").orderBy("cost_filled", "id")
w2 = Window.partitionBy("order_id", "carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("cost", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("status").orderBy(F.col("cost_filled_avg_6_prior").desc_nulls_last(), "id")

valid = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-07-27").cast("date")) & (F.col("cost") <= 54))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.concat("carrier", F.lit(", "), "status").alias("carrier_status"),
        F.coalesce("carrier", F.lit("N/A")).alias("resolved_carrier"),
    )
)

valid_shipments = (
    valid
    .filter((F.col("delivered_date") >= F.lit("2023-10-03").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "carrier_status",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("resolved_carrier_2"),
    )
)

enriched = (
    valid_shipments
    .select(
        "*",
        F.ntile(4).over(w).alias("cost_filled_bucket"),
        F.sum("cost").over(w2).alias("cumulative_cost"),
        F.first("cost", ignorenulls=True).over(w3).alias("first_known_cost"),
        F.avg("cost_filled").over(w4).alias("cost_filled_avg_6_prior"),
        F.avg("cost").over(w5).alias("cost_avg_6_prior"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.delivered_date",
        "src.cumulative_cost",
        "src.cost_filled_avg_6_prior",
        "src.cost_avg_6_prior",
        F.col("d.channel"),
        F.col("d.status"),
        F.col("d.amount"),
    )
)

ranked = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

totals = (
    ranked
    .groupBy(
        F.when(F.col("cost_avg_6_prior") >= 73, "LARGE").otherwise("SMALL").alias("cost_avg_6_prior_tier"),
        F.when(F.col("cost_filled_avg_6_prior") >= 71, "PREMIUM").otherwise("STANDARD").alias("cost_filled_avg_6_prior_tier"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("cumulative_cost")).otherwise(0)).alias("partner_cumulative_cost"),
        F.avg("cost_avg_6_prior").alias("avg_cost_avg_6_prior"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("carrier") == "Kerry", 1)), F.lit(0)), 2).alias("amount_per_kerry"),
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
    )
    .filter(F.col("partner_cumulative_cost") > 0)
)

result = (
    totals
    .select(
        "cost_avg_6_prior_tier",
        "cost_filled_avg_6_prior_tier",
        "partner_cumulative_cost",
        "avg_cost_avg_6_prior",
        "amount_per_kerry",
        "partner_events",
    )
    .orderBy(F.col("partner_events").desc_nulls_first())
)
