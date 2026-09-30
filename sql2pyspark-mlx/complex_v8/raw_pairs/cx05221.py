from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id").orderBy("read_at", "id")

in_window = (
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
        F.sum("b.voltage").alias("prior_voltage_1d"),
    )
)

deduped = (
    in_window
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .cube("site_id", "prior_rows_1d")
    .agg(
        F.sum("prior_voltage_1d").alias("total_prior_voltage_1d"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("prior_rows_1d").asc_nulls_last(),
    )
)
