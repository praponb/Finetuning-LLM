from pyspark.sql import functions as F

in_window = (
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

enriched_site = (
    in_window.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.prior_rows_7d",
        "s.prior_kwh_7d",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

labeled = (
    enriched_site
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_rows_7d",
        "prior_kwh_7d",
        "site_name",
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

labeled_2 = (
    labeled
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_rows_7d",
        "prior_kwh_7d",
        "site_name",
        "read_at_dayofweek",
        F.substring(F.trim(F.col("site_name")), 1, 3).alias("site_name_prefix"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
)

result = (
    labeled_2
    .groupingSets(
        [["prior_rows_7d", "site_id", "read_at_dayofweek"], ["prior_rows_7d"], ["site_id"], []],
        "prior_rows_7d", "site_id", "read_at_dayofweek",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(F.upper(F.trim(F.col("site_name"))).like("R%"), F.col("prior_rows_7d")).otherwise(0)).alias("prior_rows_7d_upper_trim_site_name_like_r"),
    )
    .orderBy(
        F.col("prior_rows_7d").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
        F.col("read_at_dayofweek").asc_nulls_last(),
    )
)
