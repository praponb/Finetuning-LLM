from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_rate = Window.orderBy("rate", "id")
w_rate_tercile_tariff = Window.partitionBy("rate_tercile", "tariff")
w_valid_to_month_by_next2_rate = Window.partitionBy(F.month("valid_to")).orderBy(F.col("next2_rate").asc_nulls_last())
w_dedup = Window.partitionBy("tariff").orderBy(F.col("valid_to").desc(), F.col("id").desc())

scoped_tariffs = (
    df_tariff_windows
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("valid_to")).alias("days_since_valid_to"),
        F.substring(F.trim(F.col("tariff")), 1, 3).alias("tariff_prefix"),
    )
)

with_windows = (
    scoped_tariffs
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        "tariff_slug",
        "tariff_prefix",
        F.ntile(3).over(w_all_by_rate).alias("rate_tercile"),
        F.lead("rate", 2).over(w_all_by_rate).alias("next2_rate"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        "tariff_slug",
        "tariff_prefix",
        "rate_tercile",
        "next2_rate",
        F.round(F.cume_dist().over(w_valid_to_month_by_next2_rate), 3).alias("next2_rate_cume_dist"),
        F.round(F.avg("rate_tercile").over(w_rate_tercile_tariff), 2).alias("rate_tercile_tariff_avg_rate_tercile"),
    )
)

deduped = (
    sequenced
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select("id", "rate_tercile_tariff_avg_rate_tercile", "next2_rate")
    .unpivot(
        ids=["id"],
        values=["rate_tercile_tariff_avg_rate_tercile", "next2_rate"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric_name")
)
