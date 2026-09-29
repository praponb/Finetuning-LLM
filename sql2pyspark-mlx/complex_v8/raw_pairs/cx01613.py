from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_prior_3 = Window.orderBy("read_at", "id").rowsBetween(-3, -1)
w_all_by_read_at = Window.orderBy("read_at", "id")

cleaned_readings = (
    df_meter_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.round(F.col("kwh") * 1.1, 2).alias("kwh_adj"),
    )
)

qualified = (
    cleaned_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh").between(14.4, 117.2)) & (F.month(F.col("m.read_at")).isin(2, 3)), "left_semi")
    .filter(F.col("quality").isin("EST", "OK"))
)

windowed = (
    qualified
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "kwh_adj",
        F.max("kwh_adj").over(w_all_by_read_at_prior_3).alias("kwh_adj_max_3_prior"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_read_at)).alias("voltage_change"),
        (F.col("kwh_adj") - F.lag("kwh_adj").over(w_all_by_read_at)).alias("kwh_adj_change"),
    )
)

filtered = (
    windowed
    .crossJoin(windowed.agg(F.max("voltage").alias("overall_max_voltage")))
    .filter(F.col("voltage") < F.col("overall_max_voltage"))
    .drop("overall_max_voltage")
)

annotated = (
    filtered
    .filter(F.col("quality") != "EST")
    .select(
        "*",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

result = (
    annotated
    .groupBy("read_at")
    .pivot("quality", ["BAD", "EST"])
    .agg(F.sum("kwh_adj_max_3_prior"))
    .orderBy("read_at")
)
