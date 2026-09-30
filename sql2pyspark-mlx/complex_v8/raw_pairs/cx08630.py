from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_quality_len_ties = Window.orderBy("quality_len")
w_site_commissioned_on_month_b = Window.partitionBy(F.month("site_commissioned_on")).orderBy(F.col("days_since_read_at").desc(), "id")
w_site_id = Window.partitionBy("site_id")
w_quality_len_site_commissione = Window.partitionBy("quality_len", F.year("site_commissioned_on")).orderBy("site_installed_kw", "id")
w_all_by_quality_len = Window.orderBy("quality_len", "id")
w_quality_len_site_id = Window.partitionBy("quality_len", "site_id")
w_quality_len_by_days_since_re = Window.partitionBy("quality_len").orderBy(F.col("days_since_read_at").desc(), "id")

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

enriched = (
    filtered.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (~F.col("si.zone").isin("Z3", "Z2")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.days_since_read_at",
        "s.quality_len",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "site_id",
        "days_since_read_at",
        "quality_len",
        "site_installed_kw",
        F.round(F.percent_rank().over(w_all_by_quality_len_ties), 3).alias("quality_len_percent_rank"),
        F.lead("voltage", 2, 0).over(w_site_commissioned_on_month_b).alias("next2_voltage"),
        F.min("voltage").over(w_site_id).alias("site_id_min_voltage"),
        F.rank().over(w_quality_len_site_commissione).alias("site_installed_kw_rank"),
        F.round(F.percent_rank().over(w_all_by_quality_len), 3).alias("quality_len_percent_rank_2"),
        F.round(F.avg("site_installed_kw").over(w_quality_len_site_id), 2).alias("quality_len_site_id_avg_site_installed_kw"),
        F.lag("site_installed_kw", 2).over(w_site_commissioned_on_month_b).alias("prev2_site_installed_kw"),
        F.rank().over(w_quality_len_by_days_since_re).alias("days_since_read_at_rank"),
    )
)

top_rows = (
    metrics
    .filter((F.col("site_installed_kw_rank") <= 5) & (F.col("prev2_site_installed_kw") > 96.9))
)

result = (
    top_rows
    .select(
        "id",
        "site_id",
        "quality_len",
        "site_installed_kw",
        "quality_len_percent_rank",
        "next2_voltage",
        "site_installed_kw_rank",
        "quality_len_percent_rank_2",
        "quality_len_site_id_avg_site_installed_kw",
        "prev2_site_installed_kw",
        "days_since_read_at_rank",
    )
)
