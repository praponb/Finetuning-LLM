from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w2 = Window.partitionBy("carrier").orderBy("prev_cost", "id")
w3 = Window.partitionBy(F.year("shipped_date"), "order_id").orderBy("prev_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select(
        "*",
        F.lag("cost").over(w_grp).alias("prev_cost"),
        F.lead("cost").over(w_grp).alias("next_cost"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

metrics_shipments = (
    metrics
    .select(
        "id",
        "carrier",
        "shipped_date",
        "event_seq",
        F.ntile(10).over(w2).alias("prev_cost_bucket"),
        F.sum("prev_cost").over(w3).alias("cumulative_prev_cost"),
    )
)

matched_keys = (
    metrics_shipments
    .select("id", "carrier")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date") < "2024-03-30")
        .select("id", "carrier")
    )
)

kept_rows = (
    metrics_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    kept_rows
    .select("id", "carrier")
    .intersectAll(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("id", "carrier")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "event_seq",
        "prev_cost_bucket",
        "cumulative_prev_cost",
        F.round((F.col("cumulative_prev_cost") / F.col("event_seq")), 2).alias("cumulative_prev_cost_per_event_seq"),
        F.least("cumulative_prev_cost", "prev_cost_bucket").alias("least_cumulative_prev_cost_prev_cost_bucket"),
    )
)

valid = (
    cleaned
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-06-01"))) & F.col("carrier").isNotNull())
    .select(
        "id",
        "shipped_date",
        "prev_cost_bucket",
        "cumulative_prev_cost",
        "cumulative_prev_cost_per_event_seq",
        F.greatest("prev_cost_bucket", "least_cumulative_prev_cost_prev_cost_bucket").alias("greatest_prev_cost_bucket_least_cumulative_prev_cost_prev_cost_bucket"),
        F.round((F.col("prev_cost_bucket") / F.col("cumulative_prev_cost")), 2).alias("prev_cost_bucket_per_cumulative_prev_cost"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_filled"),
    )
)

base = (
    valid
    .filter(F.col("cumulative_prev_cost_per_event_seq") <= 51)
    .select(
        "id",
        "shipped_date",
        "cumulative_prev_cost",
        "cumulative_prev_cost_per_event_seq",
        "greatest_prev_cost_bucket_least_cumulative_prev_cost_prev_cost_bucket",
        "carrier_filled",
        F.round("prev_cost_bucket", 1).alias("prev_cost_bucket_rounded"),
        F.add_months("shipped_date", 3).alias("shipped_date_plus_months"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("month", "shipped_date_plus_months").alias("activity_month"))
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
