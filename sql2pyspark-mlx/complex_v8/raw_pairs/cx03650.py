from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_on_year_move_moved_at = Window.partitionBy(F.year("opened_on"), F.to_date("move_moved_at")).orderBy("move_moved_at", "id", "move_id")
w_opened_on_year_move_moved_at_2 = Window.partitionBy(F.year("opened_on"), F.to_date("move_moved_at")).orderBy("move_moved_at", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_on_year_move_moved_at_3 = Window.partitionBy(F.year("opened_on"), F.to_date("move_moved_at")).orderBy("move_moved_at")
w_opened_on_year_move_moved_at_4 = Window.partitionBy(F.year("opened_on"), F.to_date("move_moved_at")).orderBy("move_moved_at", "id", "move_id").rowsBetween(-6, -1)

valid_warehouses = (
    df_warehouses
    .filter((F.year(F.col("opened_on")) == 2025) & (F.col("wh_code").isNotNull()))
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.date_trunc("quarter", F.col("opened_on")).alias("opened_on_quarter"),
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket"),
    )
)

detail_rows_moves = (
    valid_warehouses.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.move_type") == "ADJUST"), "inner")
    .select(
        "s.id",
        "s.wh_code",
        "s.capacity",
        "s.opened_on",
        "s.opened_on_quarter",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.moved_at").alias("move_moved_at"),
        "st.move_type",
    )
)

filtered = (
    detail_rows_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.operator") != "op_eve") & (F.col("st.move_type").isin("INBOUND", "TRANSFER")), "left_semi")
)

windowed = (
    filtered
    .select(
        "*",
        F.row_number().over(w_opened_on_year_move_moved_at).alias("move_moved_at_seq"),
        F.first("opened_on_quarter", ignorenulls=True).over(w_opened_on_year_move_moved_at_2).alias("first_known_opened_on_quarter"),
        F.rank().over(w_opened_on_year_move_moved_at).alias("move_moved_at_rank"),
        F.round(F.percent_rank().over(w_opened_on_year_move_moved_at_3), 3).alias("move_moved_at_percent_rank"),
        F.round(F.avg("move_unit_price").over(w_opened_on_year_move_moved_at_4), 2).alias("move_unit_price_avg_6_prior"),
    )
)

top_rows = (
    windowed
    .filter((F.col("move_moved_at_rank") == 1) & (F.col("opened_on") >= F.lit("2024-11-25").cast("date")))
    .drop("move_moved_at_rank")
)

exploded_detail = (
    top_rows.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.opened_on",
        "s.opened_on_quarter",
        "s.move_id",
        "s.move_moved_at",
        "s.move_moved_at_seq",
        "s.move_moved_at_percent_rank",
        F.col("st.id").alias("move_id_2"),
        F.col("st.moved_at").alias("move_moved_at_2"),
        F.col("st.unit_price").alias("move_unit_price_2"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.operator").alias("move_operator"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "move_id",
        F.col("move_unit_price_2").alias("m_move_unit_price_2"),
        F.col("move_moved_at_percent_rank").alias("m_move_moved_at_percent_rank"),
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["m_move_unit_price_2", "m_move_moved_at_percent_rank"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
