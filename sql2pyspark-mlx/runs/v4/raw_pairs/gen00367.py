from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost_scaled", "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost_scaled").desc())
w3 = Window.partitionBy("carrier").orderBy(F.col("cost_scaled").desc(), "id")
w4 = Window.partitionBy("status").orderBy(F.col("cost_rounded").desc_nulls_first())
w5 = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id").rowsBetween(-5, Window.currentRow)

filtered = (
    df_shipments
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2024-01-08"))) & F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.round((F.col("cost") * 1.1), 2).alias("cost_scaled"),
        F.round("cost", 0).alias("cost_rounded"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

filtered_shipments = (
    filtered
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2024-03-08"))) & F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "cost_scaled",
        "cost_rounded",
        F.concat_ws("-", "status", "carrier").alias("status_carrier_key"),
    )
)

scored = (
    filtered_shipments
    .withColumn("next_cost_scaled", F.lead("cost_scaled", 1).over(w))
    .withColumn("cost_scaled_percent_rank", F.percent_rank().over(w2))
)

with_windows = (
    scored
    .select(
        "*",
        F.row_number().over(w3).alias("seq_no"),
        F.percent_rank().over(w4).alias("cost_rounded_percent_rank"),
        F.avg("next_cost_scaled").over(w5).alias("next_cost_scaled_avg_5"),
    )
)

base = (
    with_windows
    .filter(F.col("cost_scaled") <= 58)
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "cost_scaled",
        "cost_rounded",
        "status_carrier_key",
        "next_cost_scaled",
        "seq_no",
        "cost_rounded_percent_rank",
        "next_cost_scaled_avg_5",
        (F.col("cost_scaled_percent_rank") + F.col("next_cost_scaled_avg_5")).alias("cost_scaled_percent_rank_plus_next_cost_scaled_avg_5"),
    )
)

filtered_shipments_2 = (
    base
    .filter((F.col("delivered_date") >= F.lit("2023-08-14").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "cost_rounded",
        "next_cost_scaled",
        "cost_rounded_percent_rank",
        "next_cost_scaled_avg_5",
        "cost_scaled_percent_rank_plus_next_cost_scaled_avg_5",
        F.round((F.col("cost_scaled") * 1.1), 2).alias("adjusted_cost_scaled"),
        F.floor("seq_no").alias("seq_no_whole"),
    )
)

result = (
    filtered_shipments_2
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        "cost_rounded",
        "next_cost_scaled",
        "cost_scaled_percent_rank_plus_next_cost_scaled_avg_5",
        "adjusted_cost_scaled",
    )
    .orderBy(F.col("next_cost_scaled").desc_nulls_last(), F.col("id").desc())
)
