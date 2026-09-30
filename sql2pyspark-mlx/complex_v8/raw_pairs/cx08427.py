from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_moved_at = Window.partitionBy("operator").orderBy("moved_at", "id")
w_move_type_by_unit_price_full = Window.partitionBy("move_type").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type = Window.partitionBy("move_type")
w_move_type_by_unit_price_ties = Window.partitionBy("move_type").orderBy("unit_price")
w_move_type_by_unit_price = Window.partitionBy("move_type").orderBy("unit_price", "id")

base = (
    df_stock_moves
    .filter((F.col("operator") != "op_ann") & (F.col("warehouse_id") <= 5))
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

with_trends = (
    base
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price",
        F.lag("unit_price", 2).over(w_operator_by_moved_at).alias("prev2_unit_price"),
        F.first("unit_price").over(w_move_type_by_unit_price_full).alias("first_unit_price"),
        F.size(F.collect_set("sku_id").over(w_move_type)).alias("distinct_sku_id_in_move_type"),
        F.rank().over(w_move_type_by_unit_price_ties).alias("unit_price_rank"),
        F.sum("unit_price").over(w_move_type).alias("move_type_sum_unit_price"),
        (F.col("unit_price") - F.lag("unit_price").over(w_move_type_by_unit_price)).alias("unit_price_change"),
    )
)

leaders = (
    with_trends
    .filter(F.col("unit_price_rank") <= 5)
    .drop("unit_price_rank")
)

derived = (
    leaders
    .filter(F.col("sku_id").isNotNull())
    .select(
        "*",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.min("qty").alias("min_move_qty"),
        F.count("*").alias("move_count"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

profiled = (
    derived.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.min_move_qty",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.last_moved_at",
    )
)

result = (
    profiled
    .filter(F.col("move_count") > 1)
    .select("unit_price")
    .distinct()
)
