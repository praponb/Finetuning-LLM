from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty = Window.orderBy("qty", "id")
w_move_type_operator_or_unknow = Window.partitionBy("move_type", "operator_or_unknown").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type = Window.partitionBy("move_type")
w_operator_or_unknown_by_qty_c = Window.partitionBy("operator_or_unknown").orderBy("qty_cume_dist", "id")

cleaned = (
    df_stock_moves
    .filter(~F.col("operator").isin("op_ann", "op_eve"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        F.concat(F.col("operator"), F.lit("#"), F.col("qty").cast("string")).alias("operator_ref"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_qty), 3).alias("qty_cume_dist"),
        F.nth_value("qty_plus_unit_price", 2).over(w_move_type_operator_or_unknow).alias("second_qty_plus_unit_price"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator_ref",
        "qty_cume_dist",
        "second_qty_plus_unit_price",
        F.max("qty").over(w_move_type).alias("move_type_max_qty"),
        F.round(F.col("qty_plus_unit_price") / F.nullif(F.sum("qty_plus_unit_price").over(w_move_type), F.lit(0)), 4).alias("qty_plus_unit_price_share_of_move_type"),
        F.ntile(3).over(w_operator_or_unknown_by_qty_c).alias("qty_cume_dist_tercile"),
    )
)

annotated = (
    sequenced_2
    .filter(F.col("qty_cume_dist") < 0.7)
    .withColumn("qty_cume_dist_tercile_bucket", F.floor(F.col("qty_cume_dist_tercile") / 2) * 2)
)

result = (
    annotated
    .rollup("move_type", "qty_cume_dist_tercile_bucket")
    .agg(
        F.grouping("move_type").alias("is_total_move_type"),
        F.max("moved_at").alias("last_moved_at"),
        F.percentile_approx("qty_plus_unit_price_share_of_move_type", 0.5).alias("median_qty_plus_unit_price_share_of_move_type"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("qty_cume_dist_tercile_bucket").asc_nulls_last(),
    )
)
