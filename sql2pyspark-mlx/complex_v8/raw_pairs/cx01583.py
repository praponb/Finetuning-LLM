from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy("kwh", "id")
w_read_at_year_by_read_at_year = Window.partitionBy(F.year("read_at")).orderBy("read_at_year", "id")
w_read_at_year_site_id = Window.partitionBy("read_at_year", "site_id")
w_read_at_year_by_read_at_year_2 = Window.partitionBy(F.year("read_at")).orderBy("read_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_readings = (
    df_meter_readings
    .filter((F.col("kwh") <= 80.8) | (F.to_date(F.col("read_at")) < F.lit("2025-03-02").cast("date")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.year(F.col("read_at")).alias("read_at_year"),
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
    )
)

qualified = (
    scoped_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
    .filter(F.col("site_id").isNotNull())
)

unique_rows = (
    qualified
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_windows = (
    unique_rows
    .select(
        "*",
        F.round(F.cume_dist().over(w_read_at_year_by_read_at_year), 3).alias("read_at_year_cume_dist"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_year_site_id), F.lit(0)), 4).alias("voltage_share_of_read_at_year_site_id"),
        F.last("read_at").over(w_read_at_year_by_read_at_year_2).alias("last_read_at"),
        F.max("read_at_year").over(w_read_at_year_site_id).alias("read_at_year_site_id_max_read_at_year"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["read_at_year", "site_id"], ["read_at_year"], []],
        "read_at_year", "site_id",
    )
    .agg(
        F.grouping("read_at_year").alias("is_total_read_at_year"),
        F.max("last_read_at").alias("last_last_read_at"),
    )
    .orderBy(
        F.col("read_at_year").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
