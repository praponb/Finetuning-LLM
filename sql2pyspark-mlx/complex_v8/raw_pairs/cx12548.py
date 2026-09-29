from pyspark.sql import functions as F

filtered = (
    df_warehouses
    .filter((F.col("city").isNotNull()) & (F.col("opened_on") >= F.lit("2024-04-13").cast("date")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.sum("unit_price").alias("total_move_unit_price"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.total_move_unit_price",
    )
)

labeled = (
    with_stats
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("city")), " ", "_")).alias("city_slug"),
        F.concat(F.col("city"), F.lit("#"), F.col("move_count").cast("string")).alias("city_ref"),
        F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
    )
)

result = (
    labeled
    .select("id", "capacity", "move_count")
    .unpivot(
        ids=["id"],
        values=["capacity", "move_count"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
