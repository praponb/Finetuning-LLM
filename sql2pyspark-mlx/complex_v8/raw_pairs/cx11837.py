from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_on_year_city_by_move = Window.partitionBy(F.year("opened_on"), "city").orderBy("move_operator_count_filled", "id")

prepared_warehouses = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("C%")) & (F.col("capacity") < 11768))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.concat(F.col("wh_code"), F.lit("#"), F.col("id").cast("string")).alias("wh_code_ref"),
        F.substring(F.trim(F.col("wh_code")), 1, 3).alias("wh_code_prefix"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("operator").isin("op_dew", "op_chan"), 1)).alias("move_operator_count"),
    )
)

summarized_rows = (
    prepared_warehouses.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

qualified = (
    summarized_rows.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.unit_price") <= 289.7) & (F.col("st.move_type").isNotNull()), "left_anti")
)

without_matches = (
    qualified.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.moved_at") < F.lit("2025-03-08 11:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("opened_on") < F.lit("2025-02-14").cast("date"))
)

scored = (
    without_matches
    .select(
        "*",
        F.coalesce(F.col("days_since_opened_on"), F.lit(0)).alias("days_since_opened_on_filled"),
        F.round(F.col("days_since_opened_on") / F.nullif(F.col("capacity"), F.lit(0)), 3).alias("days_since_opened_on_per_capacity"),
        F.coalesce(F.col("move_operator_count"), F.lit(0)).alias("move_operator_count_filled"),
    )
)

with_windows = (
    scored
    .withColumn("next_move_operator_count_filled", F.lead("move_operator_count_filled").over(w_opened_on_year_city_by_move))
    .withColumn("prev2_opened_on", F.lag("opened_on", 2).over(w_opened_on_year_city_by_move))
)

result = (
    with_windows
    .filter(F.upper(F.trim(F.col("wh_code"))).like("K%"))
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        "wh_code_ref",
        "wh_code_prefix",
        "days_since_opened_on_filled",
        "days_since_opened_on_per_capacity",
        "next_move_operator_count_filled",
        "prev2_opened_on",
    )
)
