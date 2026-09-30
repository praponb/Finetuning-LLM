from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator", "move_type").orderBy(F.col("moved_at").desc(), F.col("id").desc())
w_sku_id = Window.partitionBy("sku_id")
w_warehouse_id_move_type_len_b = Window.partitionBy("warehouse_id", "move_type_len").orderBy("move_type_len", "id")
w_operator_by_operator_len = Window.partitionBy("operator").orderBy(F.col("operator_len").asc_nulls_last(), "id")
w_warehouse_city_by_move_type = Window.partitionBy("warehouse_city").orderBy(F.col("move_type_len_filled").desc(), "id").rowsBetween(-1, -1)
w_warehouse_city_by_move_type_2 = Window.partitionBy("warehouse_city").orderBy(F.col("move_type_len_filled").desc(), "id").rowsBetween(-3, -1)
w_warehouse_id_move_type_len_f = Window.partitionBy("warehouse_id", "move_type_len_filled")

scoped = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len_2"),
    )
)

enriched = (
    scoped
    .filter(F.col("operator").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "move_type_prefix",
        "move_type_len",
        "move_type_len_2",
        F.coalesce(F.col("move_type_len"), F.lit(0)).alias("move_type_len_filled"),
        (F.floor(F.col("move_type_len") / 2) * 2).alias("move_type_len_bucket"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched_2 = (
    latest
    .select(
        "*",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

metrics = (
    enriched_2
    .select(
        "id",
        "warehouse_id",
        "move_type_prefix",
        "move_type_len_filled",
        "operator_len",
        "days_since_moved_at",
        F.sum("move_type_len_2").over(w_sku_id).alias("sku_id_sum_move_type_len_2"),
        F.round(F.percent_rank().over(w_warehouse_id_move_type_len_b), 3).alias("move_type_len_percent_rank"),
        F.lead("move_type_len_2", 2).over(w_operator_by_operator_len).alias("next2_move_type_len_2"),
    )
)

joined = (
    metrics.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.move_type_prefix",
        "s.move_type_len_filled",
        "s.days_since_moved_at",
        "s.sku_id_sum_move_type_len_2",
        "s.move_type_len_percent_rank",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

ranked = (
    joined
    .select(
        "*",
        F.round(F.avg("days_since_moved_at").over(w_warehouse_city_by_move_type), 2).alias("days_since_moved_at_avg_1_prior"),
        F.max("move_type_len_filled").over(w_warehouse_city_by_move_type_2).alias("move_type_len_filled_max_3_prior"),
        F.round(F.col("move_type_len_percent_rank") - F.avg("move_type_len_percent_rank").over(w_warehouse_id_move_type_len_f), 2).alias("move_type_len_percent_rank_vs_warehouse_id_move_"),
    )
)

result = (
    ranked
    .select(
        "id",
        "warehouse_id",
        "move_type_prefix",
        "days_since_moved_at",
        "move_type_len_percent_rank",
        "warehouse_city",
        "move_type_len_filled_max_3_prior",
        "move_type_len_percent_rank_vs_warehouse_id_move_",
        F.concat_ws(" / ", F.col("move_type_prefix"), F.col("warehouse_city")).alias("move_type_prefix_warehouse_city_label"),
    )
    .orderBy(F.col("warehouse_city").asc(), F.col("id").desc_nulls_first())
    .limit(10)
)
