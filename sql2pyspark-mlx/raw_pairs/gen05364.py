from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id")
w2 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("delivered_date"), "order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("prev_cost", "id")
w5 = Window.partitionBy(F.date_trunc("MONTH", "delivered_date"), "status").orderBy("prev_cost", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("status").orderBy("delivered_date", "id")
w7 = Window.partitionBy(F.date_trunc("month", "delivered_date"), "status").orderBy("cost")
w8 = Window.partitionBy(F.year("delivered_date"))
w9 = Window.orderBy("prev_prev_cost_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_shipments
    .select(
        "*",
        F.lag("cost", 1).over(w).alias("prev_cost"),
        F.first("carrier", ignorenulls=True).over(w2).alias("first_known_carrier"),
        F.sum("cost").over(w3).alias("running_cost"),
    )
)

base = (
    scored
    .filter((F.col("shipped_date") >= F.lit("2023-03-21").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "prev_cost",
        "first_known_carrier",
        "running_cost",
        F.coalesce("prev_cost", F.lit(0)).alias("prev_cost_filled"),
        F.round("running_cost", 2).alias("running_cost_rounded"),
    )
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

scored_shipments = (
    ranked
    .filter(F.col("prev_cost") <= 47)
    .select(
        "id",
        "delivered_date",
        "first_known_carrier",
        "rn",
        F.avg("running_cost_rounded").over(w5).alias("running_cost_rounded_avg_6_prior"),
        F.lag("prev_cost_filled", 1).over(w6).alias("prev_prev_cost_filled"),
        F.cume_dist().over(w7).alias("cost_cume_dist"),
    )
)

enriched = (
    scored_shipments
    .select(
        "id",
        "delivered_date",
        "rn",
        "running_cost_rounded_avg_6_prior",
        "cost_cume_dist",
        F.stddev_samp("running_cost_rounded_avg_6_prior").over(w8).alias("delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior"),
        F.sum("running_cost_rounded_avg_6_prior").over(w9).alias("cumulative_running_cost_rounded_avg_6_prior"),
    )
)

summary = (
    enriched
    .groupBy(
        F.date_trunc("QUARTER", "delivered_date").alias("quarter_start"),
        F.when(F.col("delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior") >= 78, "LARGE").otherwise("SMALL").alias("delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior_tier"),
    )
    .agg(
        F.percentile_approx("delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior", 0.95).alias("delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior_p95"),
        F.percentile_approx("cumulative_running_cost_rounded_avg_6_prior", 0.5).alias("cumulative_running_cost_rounded_avg_6_prior_p50"),
        F.max("delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior").alias("max_delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior"),
        F.avg("cost_cume_dist").alias("avg_cost_cume_dist"),
        F.percentile_approx("cost_cume_dist", 0.95).alias("cost_cume_dist_p95"),
    )
)

result = (
    summary
    .select(
        "quarter_start",
        "delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior_tier",
        "delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior_p95",
        "cumulative_running_cost_rounded_avg_6_prior_p50",
        "max_delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior",
        "avg_cost_cume_dist",
    )
)
