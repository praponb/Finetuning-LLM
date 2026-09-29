from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone = Window.partitionBy("zone")
w_site_name_len_by_installed_k = Window.partitionBy("site_name_len").orderBy("installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_name_len_by_installed_k_2 = Window.partitionBy("site_name_len").orderBy("installed_kw", "id")

recent = (
    df_sites
    .filter((F.col("installed_kw").between(93.3, 373.3)) & (F.col("commissioned_on") >= F.lit("2024-06-27").cast("date")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.length(F.trim(F.col("site_name"))).alias("site_name_len"),
        F.date_add(F.col("commissioned_on"), 90).alias("commissioned_on_plus_90d"),
    )
)

windowed = (
    recent
    .select(
        "*",
        F.max("installed_kw").over(w_zone).alias("zone_max_installed_kw"),
        F.first("installed_kw", ignorenulls=True).over(w_site_name_len_by_installed_k).alias("first_known_installed_kw"),
        F.row_number().over(w_site_name_len_by_installed_k_2).alias("seq_no"),
    )
)

labeled = (
    windowed
    .select(
        "id",
        "site_name",
        "zone",
        "commissioned_on",
        "site_name_len",
        "commissioned_on_plus_90d",
        "seq_no",
        (F.floor(F.col("first_known_installed_kw") / 100) * 100).alias("first_known_installed_kw_bucket"),
        F.date_trunc("month", F.col("commissioned_on_plus_90d")).alias("commissioned_on_plus_90d_month"),
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
    )
)

result = (
    labeled
    .select(
        "id",
        "site_name_len",
        "first_known_installed_kw_bucket",
        "seq_no",
    )
    .unpivot(
        ids=["id"],
        values=["site_name_len", "first_known_installed_kw_bucket", "seq_no"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
