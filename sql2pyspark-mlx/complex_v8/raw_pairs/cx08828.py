from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy("read_at")
w_site_id = Window.partitionBy("site_id")
w_prior_rows_12h_site_id_by_re = Window.partitionBy("prior_rows_12h", "site_id").orderBy(F.col("read_at").asc_nulls_last(), "id")

matched = (
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

sequenced = (
    matched
    .select(
        "*",
        F.round(F.cume_dist().over(w_site_id_by_read_at_ties), 3).alias("read_at_cume_dist"),
        F.min("prior_rows_12h").over(w_site_id).alias("site_id_min_prior_rows_12h"),
        F.round(F.percent_rank().over(w_prior_rows_12h_site_id_by_re), 3).alias("read_at_percent_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "read_at",
        "prior_rows_12h",
        "read_at_cume_dist",
    )
    .orderBy("id")
)
