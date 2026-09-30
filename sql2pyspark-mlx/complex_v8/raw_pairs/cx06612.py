from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_wh_code_len = Window.partitionBy("wh_code_len")
w_all_by_city_len_prior_3 = Window.orderBy(F.col("city_len").desc(), "id").rowsBetween(-3, -1)
w_wh_code_len_move_sku_id_by_c = Window.partitionBy("wh_code_len", "move_sku_id").orderBy(F.col("capacity_filled").desc(), "id", "move_id")
w_wh_code_len_move_operator_co = Window.partitionBy("wh_code_len", "move_operator_count")
w_wh_code_len_move_sku_id_by_c_2 = Window.partitionBy("wh_code_len", "move_sku_id").orderBy(F.col("capacity_filled").desc())
w_wh_code_len_move_sku_id_by_c_3 = Window.partitionBy("wh_code_len", "move_sku_id").orderBy(F.col("capacity_filled").desc(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_warehouses
    .filter((F.col("city").isin("Bangkok", "Singapore")) & (F.upper(F.trim(F.col("wh_code"))).like("B%")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
        F.length(F.trim(F.col("wh_code"))).alias("wh_code_len"),
        F.length(F.trim(F.col("city"))).alias("city_len"),
    )
)

metrics = (
    prepared
    .select(
        "*",
        F.round(F.col("capacity") - F.avg("capacity").over(w_wh_code_len), 2).alias("capacity_vs_wh_code_len_avg"),
        F.round(F.col("capacity_filled") / F.nullif(F.sum("capacity_filled").over(w_wh_code_len), F.lit(0)), 4).alias("capacity_filled_share_of_wh_code_len"),
        F.max("capacity_filled").over(w_all_by_city_len_prior_3).alias("capacity_filled_max_3_prior"),
        F.max("capacity_filled").over(w_wh_code_len).alias("wh_code_len_max_capacity_filled"),
    )
)

qualified = (
    metrics
    .crossJoin(metrics.agg(F.max("capacity_filled_max_3_prior").alias("overall_max_capacity_filled_max_3_prior")))
    .filter(F.col("capacity_filled_max_3_prior") < F.col("overall_max_capacity_filled_max_3_prior"))
    .drop("overall_max_capacity_filled_max_3_prior")
)

with_children_moves = (
    qualified.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.wh_code",
        "s.opened_on",
        "s.capacity_filled",
        "s.wh_code_len",
        "s.city_len",
        "s.capacity_filled_share_of_wh_code_len",
        "s.capacity_filled_max_3_prior",
        "s.wh_code_len_max_capacity_filled",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

with_parent_sku = (
    with_children_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.move_sku_id"), "inner")
    .select(
        "s.*",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count(F.when(F.col("operator").isin("op_bee", "op_chan", "op_eve"), 1)).alias("move_operator_count"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

profiled = (
    with_parent_sku.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        "st.first_moved_at",
    )
)

sequenced = (
    profiled
    .select(
        "*",
        F.ntile(4).over(w_wh_code_len_move_sku_id_by_c).alias("capacity_filled_quartile"),
        F.count(F.lit(1)).over(w_wh_code_len_move_operator_co).alias("rows_in_wh_code_len_move_operator_count"),
        F.round(F.cume_dist().over(w_wh_code_len_move_sku_id_by_c_2), 3).alias("capacity_filled_cume_dist"),
        F.rank().over(w_wh_code_len_move_sku_id_by_c_2).alias("capacity_filled_rank"),
        F.round(F.avg("move_qty").over(w_wh_code_len_move_operator_co), 2).alias("wh_code_len_move_operator_count_avg_move_qty"),
        F.first("move_operator_count").over(w_wh_code_len_move_sku_id_by_c_3).alias("first_move_operator_count"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "move_id",
        "capacity_filled_cume_dist",
        "wh_code_len_move_operator_count_avg_move_qty",
        "capacity_filled_share_of_wh_code_len",
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["capacity_filled_cume_dist", "wh_code_len_move_operator_count_avg_move_qty", "capacity_filled_share_of_wh_code_len"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
