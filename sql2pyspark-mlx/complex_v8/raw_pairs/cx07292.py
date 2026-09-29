from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_12h_site_id_by_re = Window.partitionBy("prior_rows_12h", "site_id").orderBy("read_at", "id")
w_prior_rows_12h_site_id_by_re_2 = Window.partitionBy("prior_rows_12h", "site_id").orderBy("read_at")
w_prior_rows_12h_site_id = Window.partitionBy("prior_rows_12h", "site_id")
w_all_by_prior_rows_12h = Window.orderBy("prior_rows_12h", "id")

joined_ranges = (
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
        F.sum("b.kwh").alias("prior_kwh_12h"),
    )
)

metrics = (
    joined_ranges
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_kwh_12h",
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_prior_rows_12h_site_id_by_re))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.dense_rank().over(w_prior_rows_12h_site_id_by_re_2).alias("read_at_dense_rank"),
        F.max("prior_rows_12h").over(w_prior_rows_12h_site_id).alias("prior_rows_12h_site_id_max_prior_rows_12h"),
        F.rank().over(w_all_by_prior_rows_12h).alias("prior_rows_12h_rank"),
        (F.col("prior_kwh_12h") - F.lag("prior_kwh_12h").over(w_prior_rows_12h_site_id_by_re)).alias("prior_kwh_12h_change"),
    )
)

result = (
    metrics
    .select(
        "id",
        F.col("prior_rows_12h_rank").alias("m_prior_rows_12h_rank"),
        F.col("prior_rows_12h_site_id_max_prior_rows_12h").alias("prior_rows_12h_site_id_max_prior_rows_12h_v"),
        F.col("read_at_dense_rank").alias("m_read_at_dense_rank"),
    )
    .unpivot(
        ids=["id"],
        values=["m_prior_rows_12h_rank", "prior_rows_12h_site_id_max_prior_rows_12h_v", "m_read_at_dense_rank"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "field")
)
