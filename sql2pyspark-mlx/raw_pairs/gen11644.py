from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy(F.month("delivered_date"), "carrier").orderBy("delivered_date", "id")
w_grp = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w_running = Window.orderBy("delivered_date", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("carrier").orderBy("cost_avg_3_prior", "id")
w6 = Window.orderBy("running_cost_adjusted", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("carrier").orderBy(F.col("seq_no").desc(), "id")
w8 = Window.partitionBy("carrier").orderBy("cost_adjusted_times_next_cost_adjusted")
w9 = Window.partitionBy("carrier").orderBy(F.col("cost_avg_3_prior_avg_5_prior").desc(), "id")
w10 = Window.partitionBy("carrier").orderBy("cost_avg_3_prior_avg_5_prior", "id").rowsBetween(-5, -1)
w11 = Window.partitionBy("carrier").orderBy("latest_rank", "id")
w_part = Window.partitionBy("carrier").orderBy(F.col("cost_avg_3_prior_avg_5_prior").desc_nulls_last())

valid = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-09-25").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.concat("status", F.lit("/"), "carrier").alias("status_carrier"),
        F.round((F.col("cost") * 1.07), 2).alias("cost_adjusted"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost_adjusted",
        "resolved_carrier",
        F.sum("cost_adjusted").over(w).alias("running_cost_adjusted"),
        F.lead("cost_adjusted", 1).over(w_recent).alias("next_cost_adjusted"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.avg("cost").over(w_running).alias("cost_avg_3_prior"),
    )
)

base = (
    enriched
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-05-07"))) & F.col("carrier").isin(["Kerry", "FedEx", "Flash"]))
    .withColumn("cost_adjusted_times_next_cost_adjusted", (F.col("cost_adjusted") * F.col("next_cost_adjusted")))
    .withColumn("greatest_row_seq_running_cost_adjusted", F.greatest("row_seq", "running_cost_adjusted"))
)

scored = (
    base
    .filter(F.col("shipped_date").between("2023-09-01", "2024-04-21"))
    .select(
        "*",
        F.row_number().over(w5).alias("seq_no"),
        F.avg("cost_avg_3_prior").over(w6).alias("cost_avg_3_prior_avg_5_prior"),
    )
)

latest = (
    scored
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    latest
    .withColumn("rank_in_group", F.rank().over(w8))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "carrier",
        "cost_adjusted",
        "running_cost_adjusted",
        "seq_no",
        "cost_avg_3_prior_avg_5_prior",
        "latest_rank",
    )
)

scored_shipments = (
    deduped
    .select(
        "*",
        F.row_number().over(w9).alias("event_seq"),
        F.avg("cost_avg_3_prior_avg_5_prior").over(w10).alias("cost_avg_3_prior_avg_5_prior_avg_5_prior"),
        F.lead("cost_avg_3_prior_avg_5_prior", 1).over(w11).alias("next_cost_avg_3_prior_avg_5_prior"),
        F.percent_rank().over(w_part).alias("cost_avg_3_prior_avg_5_prior_percent_rank"),
    )
)

summary = (
    scored_shipments
    .groupBy("seq_no")
    .agg(
        F.round(F.avg("running_cost_adjusted"), 2).alias("avg_running_cost_adjusted"),
        F.percentile_approx("next_cost_avg_3_prior_avg_5_prior", [0.25, 0.5, 0.75]).alias("next_cost_avg_3_prior_avg_5_prior_quartiles"),
        F.countDistinct("carrier").alias("distinct_carriers"),
    )
)

result = (
    summary
    .select(
        "seq_no",
        "avg_running_cost_adjusted",
        "next_cost_avg_3_prior_avg_5_prior_quartiles",
    )
    .orderBy(F.col("avg_running_cost_adjusted").asc_nulls_last())
)
