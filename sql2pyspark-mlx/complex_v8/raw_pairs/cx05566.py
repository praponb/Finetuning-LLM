from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_operator_move_type_by_m = Window.partitionBy("move_operator", "move_type").orderBy(F.col("move_unit_price").desc())
w_move_operator_move_type_by_m_2 = Window.partitionBy("move_operator", "move_type").orderBy(F.col("move_unit_price").desc(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_operator = Window.partitionBy("move_operator")
w_move_operator_move_type_by_m_3 = Window.partitionBy("move_operator", "move_type").orderBy(F.col("move_unit_price").desc(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("move_type").orderBy(F.col("move_unit_price").desc(), F.col("id").desc(), F.col("move_id").desc())

scoped = (
    df_warehouses
    .filter((F.col("opened_on") >= F.lit("2024-04-13").cast("date")) & (F.col("capacity") >= 4912))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.lower(F.regexp_replace(F.trim(F.col("wh_code")), " ", "_")).alias("wh_code_slug"),
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
        F.substring(F.trim(F.col("wh_code")), 1, 4).alias("wh_code_prefix"),
    )
)

line_level_moves = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.wh_code",
        "s.capacity",
        "s.wh_code_prefix",
        F.col("st.id").alias("move_id"),
        "st.move_type",
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.operator").alias("move_operator"),
    )
)

ranked = (
    line_level_moves
    .select(
        "*",
        F.dense_rank().over(w_move_operator_move_type_by_m).alias("move_unit_price_dense_rank"),
        F.nth_value("move_unit_price", 2).over(w_move_operator_move_type_by_m_2).alias("second_move_unit_price"),
        F.round(F.col("move_unit_price") - F.avg("move_unit_price").over(w_move_operator), 2).alias("move_unit_price_vs_move_operator_avg"),
        F.first("capacity", ignorenulls=True).over(w_move_operator_move_type_by_m_3).alias("first_known_capacity"),
    )
)

tagged = (
    ranked
    .select(
        "*",
        F.coalesce(F.col("move_operator"), F.lit("UNKNOWN")).alias("move_operator_or_unknown"),
        F.abs(F.col("capacity") - 11506).alias("capacity_distance"),
        F.round(F.col("second_move_unit_price") / F.nullif(F.col("first_known_capacity"), F.lit(0)), 3).alias("second_move_unit_price_per_first_known_capacity"),
    )
)

latest = (
    tagged
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select(
        "id",
        "wh_code",
        "wh_code_prefix",
        "move_id",
        "move_unit_price",
        "move_operator",
        "move_unit_price_dense_rank",
        "move_unit_price_vs_move_operator_avg",
        "capacity_distance",
        "second_move_unit_price_per_first_known_capacity",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
    .orderBy(F.col("wh_code_prefix").desc_nulls_first(), "move_type_or_unknown")
)
