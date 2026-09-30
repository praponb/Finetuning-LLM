from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage_full = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_ties = Window.orderBy(F.col("voltage").desc())
w_voltage_dense_rank = Window.partitionBy("voltage_dense_rank")
w_voltage_dense_rank_by_voltag = Window.partitionBy("voltage_dense_rank").orderBy(F.col("voltage_dense_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_voltage_dense_rank_by_voltag_2 = Window.partitionBy("voltage_dense_rank").orderBy(F.col("voltage_dense_rank").desc())

recent = (
    df_meter_readings
    .filter(F.col("voltage") >= 218.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

enriched = (
    recent
    .filter(F.col("quality").isin("EST", "OK"))
    .select(
        "id",
        "kwh",
        "voltage",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.upper(F.trim(F.col("read_at_label"))).alias("read_at_label_clean"),
        F.concat(F.col("read_at_label"), F.lit("#"), F.col("days_since_read_at").cast("string")).alias("read_at_label_ref"),
    )
)

with_trends = (
    scored
    .select(
        "id",
        "kwh",
        "days_since_read_at",
        "read_at_label_clean",
        "read_at_label_ref",
        F.last("voltage").over(w_all_by_voltage_full).alias("last_voltage"),
        F.dense_rank().over(w_all_by_voltage_ties).alias("voltage_dense_rank"),
    )
)

scored_2 = (
    with_trends
    .filter(F.col("kwh") > 46.6)
    .withColumn("read_at_label_ref_slug", F.lower(F.regexp_replace(F.trim(F.col("read_at_label_ref")), " ", "_")))
)

ranked = (
    scored_2
    .select(
        "id",
        "kwh",
        "days_since_read_at",
        "last_voltage",
        "voltage_dense_rank",
        "read_at_label_ref_slug",
        F.count(F.lit(1)).over(w_voltage_dense_rank).alias("rows_in_voltage_dense_rank"),
        F.round(F.col("voltage_dense_rank") / F.nullif(F.sum("voltage_dense_rank").over(w_voltage_dense_rank), F.lit(0)), 4).alias("voltage_dense_rank_share_of_voltage_dense_rank"),
        F.min("days_since_read_at").over(w_voltage_dense_rank_by_voltag).alias("min_days_since_read_at_so_far"),
        F.round(F.percent_rank().over(w_voltage_dense_rank_by_voltag_2), 3).alias("voltage_dense_rank_percent_rank"),
        F.round(F.col("last_voltage") / F.nullif(F.sum("last_voltage").over(w_voltage_dense_rank), F.lit(0)), 4).alias("last_voltage_share_of_voltage_dense_rank"),
        F.last("last_voltage", ignorenulls=True).over(w_voltage_dense_rank_by_voltag).alias("last_known_last_voltage"),
    )
)

leaders = (
    ranked
    .filter(F.col("voltage_dense_rank") == 1)
    .drop("voltage_dense_rank")
)

result = (
    leaders
    .select(
        "id",
        "kwh",
        "days_since_read_at",
        "last_voltage",
        "read_at_label_ref_slug",
        "rows_in_voltage_dense_rank",
        "voltage_dense_rank_share_of_voltage_dense_rank",
        "min_days_since_read_at_so_far",
        "last_voltage_share_of_voltage_dense_rank",
        "last_known_last_voltage",
        F.length(F.trim(F.col("read_at_label_ref_slug"))).alias("read_at_label_ref_slug_len"),
    )
    .orderBy("last_voltage", "last_voltage_share_of_voltage_dense_rank")
)
