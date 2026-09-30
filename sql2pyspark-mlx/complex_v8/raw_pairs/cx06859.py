from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id").orderBy("prior_rows_1d", "id")

matched = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.kwh").alias("prior_kwh_1d"),
    )
)

latest_per_key = (
    matched
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enriched = (
    latest_per_key
    .filter(F.col("prior_kwh_1d") > 240.0)
    .select(
        "*",
        F.abs(F.col("prior_kwh_1d") - 270.0).alias("prior_kwh_1d_distance"),
        (F.col("prior_rows_1d") - F.col("prior_kwh_1d")).alias("prior_rows_1d_minus_prior_kwh_1d"),
    )
)

result = (
    enriched
    .filter(F.col("prior_rows_1d_minus_prior_kwh_1d") > 182.4)
    .select(
        "id",
        "read_at",
        "prior_rows_1d",
        "prior_kwh_1d",
        F.abs(F.col("prior_rows_1d") - 3).alias("prior_rows_1d_distance"),
    )
    .orderBy(
        F.col("read_at").asc_nulls_last(),
        F.col("prior_rows_1d_distance").desc(),
        "id",
    )
    .limit(10)
)
