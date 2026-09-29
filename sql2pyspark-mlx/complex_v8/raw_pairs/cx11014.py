from pyspark.sql import functions as F

joined_ranges = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.kwh").alias("prior_kwh_7d"),
    )
)

tagged = (
    joined_ranges
    .select(
        "*",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.abs(F.col("prior_rows_7d") - 3).alias("prior_rows_7d_distance"),
    )
)

annotated = (
    tagged
    .filter(F.col("site_id").isNotNull())
    .select(
        "*",
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
        F.when(F.col("read_at_quarter") >= 3, "large").when(F.col("read_at_quarter") >= 2, "medium").otherwise("small").alias("read_at_quarter_tier"),
    )
)

result = (
    annotated
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_rows_7d",
        "read_at_quarter",
        "prior_rows_7d_distance",
        "read_at_label",
        F.concat_ws(": ", F.col("read_at_label"), F.col("read_at_quarter_tier")).alias("read_at_label_read_at_quarter_tier_label"),
    )
    .orderBy(F.col("read_at_label").asc_nulls_last())
)
