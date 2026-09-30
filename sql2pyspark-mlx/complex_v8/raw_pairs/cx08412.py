from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), F.col("id").desc())

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
        F.sum("b.voltage").alias("prior_voltage_1d"),
    )
)

latest = (
    matched
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "site_id",
        "prior_rows_1d",
        "prior_voltage_1d",
        (F.col("prior_rows_1d") - F.col("prior_voltage_1d")).alias("prior_rows_1d_minus_prior_voltage_1d"),
        F.round(F.col("prior_voltage_1d") * 1.07, 2).alias("prior_voltage_1d_adj"),
    )
    .orderBy(
        F.col("prior_rows_1d_minus_prior_voltage_1d").desc(),
        F.col("prior_rows_1d").asc_nulls_last(),
    )
)
