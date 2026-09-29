from pyspark.sql import functions as F

cleaned_warehouses = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("B%")) & (F.col("opened_on") >= F.lit("2024-04-13").cast("date")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.when(F.col("capacity") >= 11506, "A").when(F.col("capacity") >= 4912, "B").otherwise("C").alias("capacity_tier"),
        F.trunc(F.col("opened_on"), "MM").alias("opened_on_month_start"),
        F.when(F.col("capacity") >= 11506, "A").when(F.col("capacity") >= 4912, "B").otherwise("C").alias("capacity_tier_2"),
    )
)

with_activity = (
    cleaned_warehouses.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.move_type").isin("ADJUST", "OUTBOUND")) & (F.col("st.moved_at") >= F.lit("2025-01-19 12:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("opened_on_month_start").isNotNull())
)

result = (
    with_activity
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        "capacity_tier",
        "opened_on_month_start",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
    )
    .orderBy("opened_on", F.col("opened_on_month_start").desc_nulls_first())
)
