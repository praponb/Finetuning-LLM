from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_on_quarter = Window.partitionBy("opened_on_quarter")
w_capacity_tier_city_by_opened = Window.partitionBy("capacity_tier", "city").orderBy("opened_on_quarter", "id").rowsBetween(-6, -1)
w_capacity_tier_city_by_opened_2 = Window.partitionBy("capacity_tier", "city").orderBy("opened_on_quarter", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_warehouses
    .filter((F.col("opened_on") < F.lit("2025-02-14").cast("date")) & (F.col("capacity") < 11768))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.when(F.col("city").isin("Singapore", "Bangkok"), "group_a").when(F.col("city") == "Kuala Lumpur", "group_b").otherwise("other").alias("city_group"),
        F.when(F.col("capacity") >= 11506, "A").when(F.col("capacity") >= 11005, "B").otherwise("C").alias("capacity_tier"),
        F.quarter(F.col("opened_on")).alias("opened_on_quarter"),
    )
)

with_activity = (
    valid.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.move_type") == "TRANSFER"), "left_semi")
)

sequenced = (
    with_activity
    .select(
        "*",
        F.count(F.lit(1)).over(w_opened_on_quarter).alias("rows_in_opened_on_quarter"),
        F.round(F.avg("capacity").over(w_capacity_tier_city_by_opened), 2).alias("capacity_avg_6_prior"),
        F.round(F.col("capacity") / F.nullif(F.sum("capacity").over(w_opened_on_quarter), F.lit(0)), 4).alias("capacity_share_of_opened_on_quarter"),
        F.max("capacity").over(w_opened_on_quarter).alias("opened_on_quarter_max_capacity"),
        F.round(F.avg("capacity").over(w_capacity_tier_city_by_opened_2), 2).alias("capacity_avg_7"),
    )
)

qualified = (
    sequenced.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.operator").isin("op_dew", "op_chan", "op_eve")) & (F.hour(F.col("st.moved_at")).between(10, 15)), "left_anti")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("move_type").isin("INBOUND", "TRANSFER"), 1)).alias("move_move_type_count"),
    )
)

profiled = (
    qualified.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

rollup_base = (
    profiled
    .groupBy("capacity_tier")
    .agg(
        F.round(100 * F.count(F.when(~F.col("city").isin("Bangkok", "Singapore"), 1)) / F.count("*"), 1).alias("pct_city_not_in_bangkok_singapor"),
        F.min("capacity_avg_7").alias("min_capacity_avg_7"),
        F.count("*").alias("row_count"),
        F.max("capacity_avg_6_prior").alias("max_capacity_avg_6_prior"),
        F.sum(F.when(F.col("city") == "Kuala Lumpur", F.col("opened_on_quarter_max_capacity"))).alias("opened_on_quarter_max_capacity_city_kuala_lumpur"),
    )
)

result = (
    rollup_base
    .select(
        "capacity_tier",
        "pct_city_not_in_bangkok_singapor",
        "row_count",
        F.when(F.col("capacity_tier").isin("B", "A"), "group_a").when(F.col("capacity_tier") == "C", "group_b").otherwise("other").alias("capacity_tier_group"),
    )
    .orderBy(F.col("pct_city_not_in_bangkok_singapor").desc())
)
