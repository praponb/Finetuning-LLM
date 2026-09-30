from pyspark.sql import functions as F

cleaned = (
    df_sites
    .filter(~F.col("tariff").isin("TOU", "EV"))
    .select(
        "id",
        "site_name",
        "installed_kw",
        "commissioned_on",
        F.when(F.col("tariff").isin("FLAT", "TOU"), "group_a").when(F.col("tariff") == "EV", "group_b").otherwise("other").alias("tariff_group"),
        F.round(F.col("installed_kw") * 1.07, 2).alias("installed_kw_adj"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on"),
    )
)

scored = (
    cleaned
    .filter(F.col("installed_kw_adj") >= 265.8)
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on_2"),
        F.date_trunc("quarter", F.col("commissioned_on")).alias("commissioned_on_quarter"),
    )
)

result = (
    scored
    .groupBy("commissioned_on_quarter", "commissioned_on")
    .pivot("tariff_group", ["other", "group_a", "group_b"])
    .agg(F.sum("installed_kw").alias("total"), F.max("installed_kw").alias("max_v"))
    .orderBy("commissioned_on_quarter", "commissioned_on")
)
