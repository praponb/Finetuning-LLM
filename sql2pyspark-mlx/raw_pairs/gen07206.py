from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("weeks_shipped_date_to_delivered_date").asc_nulls_last(), "id")

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-03-29").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        (F.datediff("delivered_date", "shipped_date") / 7).alias("weeks_shipped_date_to_delivered_date"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "carrier",
        "shipped_date",
        "weeks_shipped_date_to_delivered_date",
        "carrier_filled",
    )
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

remaining_keys = (
    ranked
    .select("carrier", "id")
    .intersectAll(
        df_shipments
        .filter(F.col("order_id") == 26)
        .select("carrier", "id")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("shipped_date") >= F.lit("2023-03-02").cast("date"))
    .select(
        "shipped_date",
        "carrier_filled",
        "dedup_rank",
        F.round((F.col("weeks_shipped_date_to_delivered_date") * 100), 2).alias("weeks_shipped_date_to_delivered_date_adjusted"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("weeks_shipped_date_to_delivered_date_adjusted") >= 87, "LARGE").otherwise("SMALL").alias("weeks_shipped_date_to_delivered_date_adjusted_tier"),
        F.date_trunc("QUARTER", "shipped_date").alias("quarter_start"),
    )
    .agg(
        F.round(F.avg("weeks_shipped_date_to_delivered_date_adjusted"), 2).alias("avg_weeks_shipped_date_to_delivered_date_adjusted"),
        F.round(F.avg("weeks_shipped_date_to_delivered_date_adjusted"), 2).alias("avg_weeks_shipped_date_to_delivered_date_adjusted_2"),
        F.percentile_approx("weeks_shipped_date_to_delivered_date_adjusted", 0.95).alias("weeks_shipped_date_to_delivered_date_adjusted_p95"),
        F.percentile_approx("weeks_shipped_date_to_delivered_date_adjusted", [0.25, 0.5, 0.75]).alias("weeks_shipped_date_to_delivered_date_adjusted_quartiles"),
    )
    .filter(F.col("weeks_shipped_date_to_delivered_date_adjusted_p95") < 1000000)
    .orderBy(
        F.col("avg_weeks_shipped_date_to_delivered_date_adjusted").desc_nulls_first(),
    )
)
