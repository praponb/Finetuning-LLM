from pyspark.sql import functions as F

valid_warehouses = (
    df_warehouses
    .filter((F.col("opened_on") >= F.lit("2024-04-13").cast("date")) & (F.length(F.trim(F.col("wh_code"))) >= 4))
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        F.lower(F.regexp_replace(F.trim(F.col("city")), " ", "_")).alias("city_slug"),
    )
)

tagged = (
    valid_warehouses
    .select(
        "*",
        F.substring(F.trim(F.col("city")), 1, 2).alias("city_prefix"),
        F.add_months(F.col("opened_on"), 6).alias("opened_on_plus_6m"),
    )
)

result = (
    tagged
    .select(
        "id",
        "city",
        "opened_on",
        "city_prefix",
        "opened_on_plus_6m",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
        F.when(F.col("city").isin("Bangkok", "Singapore"), "group_a").when(F.col("city") == "Kuala Lumpur", "group_b").otherwise("other").alias("city_group"),
    )
    .orderBy("opened_on_plus_6m", F.col("city_prefix").asc_nulls_last())
)
