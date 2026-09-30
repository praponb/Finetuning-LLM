from pyspark.sql import functions as F

recent_warehouses = (
    df_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("C%"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.quarter(F.col("opened_on")).alias("opened_on_quarter"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
        F.when(F.col("city").isin("Kuala Lumpur", "Bangkok"), "group_a").when(F.col("city") == "Singapore", "group_b").otherwise("other").alias("city_group"),
    )
)

enriched = (
    recent_warehouses
    .filter(F.col("capacity") >= 2614)
    .select(
        "*",
        F.date_trunc("quarter", F.col("opened_on")).alias("opened_on_quarter_2"),
    )
)

result = (
    enriched
    .groupBy("city_group")
    .pivot("city", ["Bangkok", "Singapore"])
    .agg(F.min("opened_on_quarter"))
)
