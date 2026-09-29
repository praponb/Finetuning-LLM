from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_or_unknown = Window.partitionBy("move_type_or_unknown")
w_all_by_qty_prior_2 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-2, -1)

recent = (
    df_stock_moves
    .filter(F.col("move_type").isin("ADJUST", "INBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

with_trends = (
    recent
    .select(
        "*",
        F.size(F.collect_set("move_type").over(w_move_type_or_unknown)).alias("distinct_move_type_in_move_type_or_unknown"),
        F.max("qty").over(w_move_type_or_unknown).alias("move_type_or_unknown_max_qty"),
        F.max("unit_price_bucket").over(w_all_by_qty_prior_2).alias("unit_price_bucket_max_2_prior"),
        F.round(F.col("unit_price_bucket") / F.nullif(F.sum("unit_price_bucket").over(w_move_type_or_unknown), F.lit(0)), 4).alias("unit_price_bucket_share_of_move_type_or_unknown"),
    )
)

tagged = (
    with_trends
    .select(
        "*",
        F.concat_ws("-", F.col("move_type_or_unknown"), F.col("move_type")).alias("move_type_or_unknown_move_type_label"),
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
    )
)

result = (
    tagged
    .select(
        "id",
        "sku_id",
        "qty",
        "move_type_or_unknown",
        "distinct_move_type_in_move_type_or_unknown",
        "move_type_or_unknown_max_qty",
        "unit_price_bucket_max_2_prior",
        "unit_price_bucket_share_of_move_type_or_unknown",
        "moved_at_day",
        F.length(F.trim(F.col("move_type_or_unknown"))).alias("move_type_or_unknown_len"),
    )
)
