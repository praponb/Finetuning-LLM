from pyspark.sql import functions as F

valid_sites = (
    df_sites
    .filter((F.col("tariff") == "FLAT") & (F.lower(F.col("site_name")).like("%la%")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.month(F.col("commissioned_on")).alias("commissioned_on_month"),
        F.round(F.col("installed_kw") * 1.07, 2).alias("installed_kw_adj"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on"),
    )
)

labeled = (
    valid_sites
    .filter(F.col("commissioned_on") < F.lit("2024-11-21").cast("date"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        "commissioned_on_month",
        "installed_kw_adj",
        "days_since_commissioned_on",
        F.when(F.col("installed_kw") >= 284.0, "HIGH").when(F.col("installed_kw") >= 141.6, "MEDIUM").otherwise("LOW").alias("installed_kw_tier"),
        F.date_trunc("month", F.col("commissioned_on")).alias("commissioned_on_month_2"),
        F.date_format(F.col("commissioned_on"), "MMM yyyy").alias("commissioned_on_label"),
    )
)

result = (
    labeled
    .groupBy("commissioned_on", "installed_kw_tier")
    .pivot("tariff", ["TOU", "EV", "FLAT"])
    .agg(F.min("installed_kw_adj"))
    .orderBy("commissioned_on", "installed_kw_tier")
)
