from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_warehouse_city = Window.partitionBy("warehouse_id", "warehouse_city").orderBy(F.col("sku_weight_kg").desc())
w_warehouse_id_warehouse_city_2 = Window.partitionBy("warehouse_id", "warehouse_city").orderBy(F.col("sku_weight_kg").desc(), "id")
w_warehouse_id_warehouse_city_3 = Window.partitionBy("warehouse_id", "warehouse_city").orderBy(F.col("sku_weight_kg").desc(), "id").rowsBetween(-3, Window.currentRow)
w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")
w_operator_by_sku_weight_kg = Window.partitionBy("operator").orderBy(F.col("sku_weight_kg").desc_nulls_first(), "id")
w_warehouse_id_warehouse_city_4 = Window.partitionBy("warehouse_id", "warehouse_city").orderBy(F.col("sku_weight_kg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_sku_weight_kg_rank = Window.partitionBy("move_type", "sku_weight_kg_rank")
w_warehouse_city_seq_no_by_sku = Window.partitionBy("warehouse_city", "seq_no").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(-6, Window.currentRow)
w_warehouse_city_seq_no_by_sku_2 = Window.partitionBy("warehouse_city", "seq_no").orderBy(F.col("sku_unit_cost").desc())
w_warehouse_city_seq_no_by_sku_3 = Window.partitionBy("warehouse_city", "seq_no").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(-1, -1)

valid_moves = (
    df_stock_moves
    .filter(F.col("move_type") != "OUTBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

joined_sku = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (~F.col("sk.category").isin("Electronics", "Beauty")), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("w.city").alias("warehouse_city"),
    )
)

with_windows = (
    joined_sku
    .select(
        "*",
        F.dense_rank().over(w_warehouse_id_warehouse_city).alias("sku_weight_kg_dense_rank"),
        F.ntile(5).over(w_warehouse_id_warehouse_city_2).alias("sku_weight_kg_quintile"),
        F.sum("sku_unit_cost").over(w_warehouse_id_warehouse_city_3).alias("sku_unit_cost_sum_4"),
        F.max("sku_weight_kg").over(w_warehouse_id_operator).alias("warehouse_id_operator_max_sku_weight_kg"),
        F.round(F.avg("sku_unit_cost").over(w_warehouse_id_operator), 2).alias("warehouse_id_operator_avg_sku_unit_cost"),
        F.row_number().over(w_operator_by_sku_weight_kg).alias("seq_no"),
        F.first("sku_weight_kg").over(w_warehouse_id_warehouse_city_4).alias("first_sku_weight_kg"),
        F.rank().over(w_warehouse_id_warehouse_city_2).alias("sku_weight_kg_rank"),
    )
)

top_rows = (
    with_windows
    .filter(F.col("sku_weight_kg_dense_rank") <= 2)
    .drop("sku_weight_kg_dense_rank")
)

metrics = (
    top_rows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "sku_weight_kg_quintile",
        "warehouse_id_operator_max_sku_weight_kg",
        "warehouse_id_operator_avg_sku_unit_cost",
        "seq_no",
        "first_sku_weight_kg",
        "sku_weight_kg_rank",
        F.size(F.collect_set("sku_id").over(w_move_type_sku_weight_kg_rank)).alias("distinct_sku_id_in_move_type_sku_weight_kg_rank"),
        F.size(F.collect_set("operator").over(w_move_type_sku_weight_kg_rank)).alias("distinct_operator_in_move_type_sku_weight_kg_ran"),
        F.round(F.avg("sku_weight_kg").over(w_warehouse_city_seq_no_by_sku), 2).alias("sku_weight_kg_avg_7"),
        F.round(F.percent_rank().over(w_warehouse_city_seq_no_by_sku_2), 3).alias("sku_unit_cost_percent_rank"),
        F.round(F.avg("first_sku_weight_kg").over(w_warehouse_city_seq_no_by_sku_3), 2).alias("first_sku_weight_kg_avg_1_prior"),
    )
)

result = (
    metrics
    .filter(F.col("sku_weight_kg_rank") >= 5)
    .select(
        "id",
        "sku_id",
        "move_type",
        "warehouse_id_operator_max_sku_weight_kg",
        "warehouse_id_operator_avg_sku_unit_cost",
        "seq_no",
        "first_sku_weight_kg",
        "sku_weight_kg_rank",
        "distinct_operator_in_move_type_sku_weight_kg_ran",
        "sku_weight_kg_avg_7",
        "sku_unit_cost_percent_rank",
        "first_sku_weight_kg_avg_1_prior",
        F.greatest(F.col("first_sku_weight_kg"), F.col("sku_weight_kg_quintile")).alias("greatest_first_sku_weight_kg_sku_weight_kg_quint"),
        F.round(F.col("sku_weight_kg_avg_7") / F.nullif(F.col("seq_no"), F.lit(0)), 3).alias("sku_weight_kg_avg_7_per_seq_no"),
    )
    .orderBy(
        F.col("seq_no").asc_nulls_last(),
        F.col("first_sku_weight_kg_avg_1_prior").desc(),
    )
)
