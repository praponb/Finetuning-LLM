from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("city").orderBy(F.col("opened_on_quarter").desc(), F.col("id").desc())

prepared_warehouses = (
    df_warehouses
    .filter((F.col("capacity") >= 2614) & (F.col("city").isin("Singapore", "Kuala Lumpur")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.date_trunc("quarter", F.col("opened_on")).alias("opened_on_quarter"),
        F.concat(F.col("wh_code"), F.lit("#"), F.col("id").cast("string")).alias("wh_code_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("wh_code")), " ", "_")).alias("wh_code_slug"),
    )
)

filtered = (
    prepared_warehouses
    .crossJoin(prepared_warehouses.agg(F.avg("capacity").alias("overall_avg_capacity")))
    .filter(F.col("capacity") > F.col("overall_avg_capacity"))
    .drop("overall_avg_capacity")
)

latest = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

screened = (
    latest.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left_semi")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.max("moved_at").alias("last_moved_at_2"),
        F.count(F.when(F.col("operator") != "op_eve", 1)).alias("move_operator_count"),
    )
)

with_stats = (
    screened.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        "st.last_moved_at_2",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("operator") == "op_dew", 1)).alias("move_operator_count_2"),
    )
)

summarized_rows = (
    with_stats.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count_2"), F.lit(0)).alias("move_operator_count_2"),
    )
)

without_matches = (
    summarized_rows
    .crossJoin(summarized_rows.agg(F.min("capacity").alias("overall_min_capacity")))
    .filter(F.col("capacity") > F.col("overall_min_capacity"))
    .drop("overall_min_capacity")
)

result = (
    without_matches
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        "opened_on_quarter",
        "last_moved_at_2",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_moved_at")).alias("days_since_last_moved_at"),
    )
    .orderBy(F.col("id").asc(), "opened_on_quarter")
)
