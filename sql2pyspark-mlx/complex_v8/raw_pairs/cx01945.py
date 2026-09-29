from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").desc(), "id")
w_sku_id_by_moved_at_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_move_type_warehouse_id_by_mo = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("moved_at").desc())
w_move_type_sku_id_by_moved_at = Window.partitionBy("move_type", "sku_id").orderBy(F.col("moved_at").asc_nulls_last())
w_move_type = Window.partitionBy("move_type")
w_all_by_moved_at_tercile_ties = Window.orderBy(F.col("moved_at_tercile").desc())
w_warehouse_id_by_moved_at_ter = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at_tercile_2").desc())
w_all_by_moved_at_tercile = Window.orderBy(F.col("moved_at_tercile").desc(), "id")
w_warehouse_id_by_moved_at_ter_2 = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at_tercile_2").desc(), "id")

cleaned_moves = (
    df_stock_moves
    .filter(F.col("qty").between(137, 415))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
    )
)

joined = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator_slug",
        "s.moved_at_label",
        "sk.sku_code",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.category").alias("sku_category"),
    )
)

with_trends = (
    joined
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "operator_slug",
        "sku_code",
        "sku_is_active",
        "sku_category",
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_warehouse_id_by_moved_at))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.last("moved_at", ignorenulls=True).over(w_sku_id_by_moved_at_running).alias("last_known_moved_at"),
        F.lead("moved_at", 3).over(w_warehouse_id_by_moved_at).alias("next3_moved_at"),
        F.ntile(3).over(w_sku_id_by_moved_at).alias("moved_at_tercile"),
        F.round(F.percent_rank().over(w_move_type_warehouse_id_by_mo), 3).alias("moved_at_percent_rank"),
        F.rank().over(w_move_type_sku_id_by_moved_at).alias("moved_at_rank"),
        F.lead("moved_at").over(w_warehouse_id_by_moved_at).alias("next_moved_at"),
        F.ntile(3).over(w_warehouse_id_by_moved_at).alias("moved_at_tercile_2"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.round(F.col("minutes_since_prev_moved_at") / F.nullif(F.sum("minutes_since_prev_moved_at").over(w_move_type), F.lit(0)), 4).alias("minutes_since_prev_moved_at_share_of_move_type"),
        F.round(F.percent_rank().over(w_all_by_moved_at_tercile_ties), 3).alias("moved_at_tercile_percent_rank"),
        F.dense_rank().over(w_warehouse_id_by_moved_at_ter).alias("moved_at_tercile_2_dense_rank"),
        F.row_number().over(w_all_by_moved_at_tercile).alias("rn"),
        F.size(F.collect_set("sku_category").over(w_move_type)).alias("distinct_sku_category_in_move_type"),
        F.round(F.col("moved_at_tercile") / F.nullif(F.sum("moved_at_tercile").over(w_move_type), F.lit(0)), 4).alias("moved_at_tercile_share_of_move_type"),
        F.round(F.cume_dist().over(w_all_by_moved_at_tercile_ties), 3).alias("moved_at_tercile_cume_dist"),
        F.ntile(5).over(w_warehouse_id_by_moved_at_ter_2).alias("moved_at_tercile_2_quintile"),
    )
)

stats = (
    metrics
    .groupBy(
        "sku_is_active",
        F.date_trunc("month", "next3_moved_at").alias("next3_moved_at_month"),
        "distinct_sku_category_in_move_type",
    )
    .agg(
        F.sum(F.when(F.col("move_type") != "OUTBOUND", 1).otherwise(0)).alias("n_move_type_outbound"),
        F.sum(F.when(~F.col("sku_category").isin("Electronics", "Beauty"), 1).otherwise(0)).alias("n_sku_category_not_in_electron"),
        F.round(F.sum("minutes_since_prev_moved_at") / F.nullif(F.sum("moved_at_percent_rank"), F.lit(0)), 3).alias("minutes_since_prev_moved_at_to_moved_at_percent_"),
        F.round(F.sum("moved_at_percent_rank"), 2).alias("total_moved_at_percent_rank"),
        F.sum("minutes_since_prev_moved_at").alias("sum_minutes_since_prev_moved_at_check"),
    )
    .filter(F.col("sum_minutes_since_prev_moved_at_check") > 1750.0)
    .drop("sum_minutes_since_prev_moved_at_check")
)

result = (
    stats
    .select(
        "sku_is_active",
        "distinct_sku_category_in_move_type",
        "n_move_type_outbound",
        "n_sku_category_not_in_electron",
        "minutes_since_prev_moved_at_to_moved_at_percent_",
        "total_moved_at_percent_rank",
    )
    .orderBy("n_sku_category_not_in_electron", "total_moved_at_percent_rank")
)
