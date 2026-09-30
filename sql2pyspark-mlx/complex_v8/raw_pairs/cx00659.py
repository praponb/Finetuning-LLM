from pyspark.sql import functions as F

filtered_warehouses = (
    df_warehouses
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        F.date_trunc("week", F.col("opened_on")).alias("opened_on_week"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
        F.substring(F.trim(F.col("wh_code")), 1, 4).alias("wh_code_prefix"),
    )
)

labeled = (
    filtered_warehouses
    .filter(F.col("city").isin("Singapore", "Kuala Lumpur"))
    .select(
        "id",
        "wh_code",
        "opened_on",
        "days_since_opened_on",
        F.add_months(F.col("opened_on"), 1).alias("opened_on_plus_1m"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_on_week")).alias("days_since_opened_on_week"),
        F.upper(F.trim(F.col("wh_code"))).alias("wh_code_clean"),
    )
)

result = (
    labeled
    .select("id", "days_since_opened_on_week", "days_since_opened_on")
    .unpivot(
        ids=["id"],
        values=["days_since_opened_on_week", "days_since_opened_on"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
