from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_by_site_commission = Window.partitionBy("site_zone").orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_zone = Window.partitionBy("site_zone")
w_site_zone_by_site_commission_2 = Window.partitionBy("site_zone").orderBy("site_commissioned_on", "id")
w_dedup = Window.partitionBy("site_id").orderBy("kwh", "id")

recent = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("kwh") <= 92.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

joined = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.voltage_plus_kwh",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "site_commissioned_on",
        "site_zone",
        F.first("site_commissioned_on", ignorenulls=True).over(w_site_zone_by_site_commission).alias("first_known_site_commissioned_on"),
        F.sum("voltage_plus_kwh").over(w_site_zone).alias("site_zone_sum_voltage_plus_kwh"),
        F.dense_rank().over(w_site_zone_by_site_commission_2).alias("site_commissioned_on_dense_rank"),
    )
)

deduped = (
    with_windows
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

derived = (
    deduped
    .filter(F.col("site_commissioned_on") < F.lit("2025-05-10").cast("date"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "site_zone",
        "first_known_site_commissioned_on",
        "site_zone_sum_voltage_plus_kwh",
        "site_commissioned_on_dense_rank",
        F.when(F.col("kwh") >= 90.6, "large").when(F.col("kwh") >= 52.5, "medium").otherwise("small").alias("kwh_tier"),
    )
)

result = (
    derived
    .select(
        "id",
        "kwh",
        "first_known_site_commissioned_on",
        "site_commissioned_on_dense_rank",
        "kwh_tier",
    )
    .orderBy(
        F.col("site_commissioned_on_dense_rank").desc_nulls_first(),
        F.col("kwh").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
