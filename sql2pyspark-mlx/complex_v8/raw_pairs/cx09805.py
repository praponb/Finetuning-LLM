from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_by_kwh_prior_3 = Window.partitionBy(F.month("read_at")).orderBy("kwh", "id").rowsBetween(-3, -1)
w_site_id_by_read_at_moving_3 = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_read_at_year = Window.partitionBy(F.year("read_at"))
w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy(F.col("read_at").desc())
w_dedup = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), F.col("id").desc())

base_readings = (
    df_meter_readings
    .filter((F.col("quality").isin("EST", "OK")) & (F.col("read_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

with_trends = (
    base_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.round(F.avg("voltage").over(w_read_at_month_by_kwh_prior_3), 2).alias("voltage_avg_3_prior"),
        F.max("voltage").over(w_site_id_by_read_at_moving_3).alias("voltage_max_4"),
        F.sum("kwh").over(w_read_at_year).alias("read_at_year_sum_kwh"),
        F.dense_rank().over(w_site_id_by_read_at_ties).alias("read_at_dense_rank"),
    )
)

latest_per_key = (
    with_trends
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

top_per_group = latest_per_key.filter(F.col("read_at_dense_rank") <= 2)

result = (
    top_per_group
    .select("id", "voltage", "kwh")
    .unpivot(
        ids=["id"],
        values=["voltage", "kwh"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
)
