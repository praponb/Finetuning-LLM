from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_warehouse_op = Window.partitionBy("warehouse_id").orderBy("warehouse_opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_is_active_sku_id_by_sku = Window.partitionBy("sku_is_active", "sku_id").orderBy("sku_weight_kg", "id").rowsBetween(-1, -1)
w_warehouse_id_sku_id_by_moved = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-3, -1)
w_sku_is_active_sku_id_by_sku_2 = Window.partitionBy("sku_is_active", "sku_id").orderBy("sku_weight_kg", "id")
w_moved_at_month_warehouse_ope = Window.partitionBy(F.date_trunc("month", "moved_at"), F.date_trunc("month", "warehouse_opened_on"))
w_warehouse_id_by_warehouse_op_2 = Window.partitionBy("warehouse_id").orderBy("warehouse_opened_on", "id")
w_warehouse_id_sku_id_by_moved_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at").desc(), "id")
w_warehouse_id_sku_is_active_b = Window.partitionBy("warehouse_id", "sku_is_active").orderBy("moved_at")
w_warehouse_id_sku_id_by_moved_3 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_is_active = Window.partitionBy("sku_is_active")
w_all_by_second_warehouse_open = Window.orderBy(F.col("second_warehouse_opened_on").asc_nulls_last(), "id")
w_warehouse_opened_on_month_by = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on")).orderBy(F.col("second_warehouse_opened_on").desc(), "id")
w_sku_is_active_seq_no = Window.partitionBy("sku_is_active", "seq_no")
w_seq_no_second_warehouse_open = Window.partitionBy("seq_no", F.date_trunc("month", "second_warehouse_opened_on")).orderBy(F.col("sku_weight_kg_max_1_prior").asc_nulls_last(), "id")
w_warehouse_opened_on_month_by_2 = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on")).orderBy(F.col("second_warehouse_opened_on").desc())

cleaned = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 7 DAYS")).alias("moved_at_plus_7d"),
    )
)

with_details = (
    cleaned.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

sequenced = (
    with_details
    .select(
        "*",
        F.nth_value("warehouse_opened_on", 2).over(w_warehouse_id_by_warehouse_op).alias("second_warehouse_opened_on"),
        F.max("sku_weight_kg").over(w_sku_is_active_sku_id_by_sku).alias("sku_weight_kg_max_1_prior"),
        F.round(F.avg("sku_weight_kg").over(w_warehouse_id_sku_id_by_moved), 2).alias("sku_weight_kg_avg_3_prior"),
        F.dense_rank().over(w_sku_is_active_sku_id_by_sku_2).alias("sku_weight_kg_dense_rank"),
        F.count(F.lit(1)).over(w_moved_at_month_warehouse_ope).alias("rows_in_moved_at_month_warehouse_opened_on_month"),
        F.ntile(10).over(w_warehouse_id_by_warehouse_op_2).alias("warehouse_opened_on_decile"),
        F.row_number().over(w_warehouse_id_sku_id_by_moved_2).alias("seq_no"),
        F.last("sku_weight_kg").over(w_warehouse_id_by_warehouse_op).alias("last_sku_weight_kg"),
        F.rank().over(w_warehouse_id_sku_is_active_b).alias("moved_at_rank"),
        F.round(F.col("sku_weight_kg") - F.avg("sku_weight_kg").over(w_moved_at_month_warehouse_ope), 2).alias("sku_weight_kg_vs_moved_at_month_warehouse_opened"),
        F.first("warehouse_opened_on").over(w_warehouse_id_by_warehouse_op).alias("first_warehouse_opened_on"),
        F.count(F.lit(1)).over(w_warehouse_id_sku_id_by_moved_3).alias("running_count"),
    )
)

ranked = (
    sequenced
    .select(
        "*",
        F.round(F.col("sku_weight_kg_avg_3_prior") / F.nullif(F.sum("sku_weight_kg_avg_3_prior").over(w_sku_is_active), F.lit(0)), 4).alias("sku_weight_kg_avg_3_prior_share_of_sku_is_active"),
        F.rank().over(w_all_by_second_warehouse_open).alias("second_warehouse_opened_on_rank"),
        F.row_number().over(w_warehouse_opened_on_month_by).alias("rn"),
        F.size(F.collect_set("sku_id").over(w_sku_is_active_seq_no)).alias("distinct_sku_id_in_sku_is_active_seq_no"),
        F.row_number().over(w_seq_no_second_warehouse_open).alias("seq_no_2"),
        F.lead("first_warehouse_opened_on").over(w_warehouse_opened_on_month_by).alias("next_first_warehouse_opened_on"),
        F.dense_rank().over(w_warehouse_opened_on_month_by_2).alias("second_warehouse_opened_on_dense_rank"),
    )
)

per_group = (
    ranked
    .groupBy(
        "warehouse_id",
        F.date_format("warehouse_opened_on", "yyyy-MM").alias("warehouse_opened_on_ym"),
    )
    .agg(
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.count("*").alias("row_count"),
        F.round(F.sum("sku_weight_kg_dense_rank") / F.nullif(F.sum("sku_weight_kg_avg_3_prior_share_of_sku_is_active"), F.lit(0)), 3).alias("sku_weight_kg_dense_rank_to_sku_weight_kg_avg_3_"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("sku_weight_kg") / F.nullif(F.sum("distinct_sku_id_in_sku_is_active_seq_no"), F.lit(0)), 3).alias("sku_weight_kg_to_distinct_sku_id_in_sku_is_activ"),
    )
)

result = (
    per_group
    .select(
        "warehouse_id",
        "distinct_sku_ids",
        "row_count",
        "sku_weight_kg_dense_rank_to_sku_weight_kg_avg_3_",
        "distinct_ids",
        "sku_weight_kg_to_distinct_sku_id_in_sku_is_activ",
        F.initcap(F.trim(F.col("warehouse_opened_on_ym"))).alias("warehouse_opened_on_ym_clean"),
        F.substring(F.trim(F.col("warehouse_opened_on_ym")), 1, 4).alias("warehouse_opened_on_ym_prefix"),
    )
)
