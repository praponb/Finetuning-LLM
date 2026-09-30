from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("prior_voltage_12h").desc(), F.col("id").desc())

covered = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.voltage").alias("prior_voltage_12h"),
    )
)

deduped = (
    covered
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

joined = (
    deduped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.prior_rows_12h",
        "s.prior_voltage_12h",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

summary = (
    joined
    .groupBy(F.year("read_at").alias("read_at_year"))
    .agg(
        F.round(F.avg("prior_rows_12h"), 2).alias("avg_prior_rows_12h"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    summary
    .select(
        "read_at_year",
        "avg_prior_rows_12h",
        "n_rows",
        F.greatest(F.col("read_at_year"), F.col("n_rows")).alias("greatest_read_at_year_n_rows"),
    )
    .orderBy(F.col("greatest_read_at_year_n_rows").desc())
)
