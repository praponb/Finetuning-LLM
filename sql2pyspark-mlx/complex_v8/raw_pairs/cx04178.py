from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_distinct_ids_by_row_c = Window.partitionBy("sku_id", "distinct_ids").orderBy("row_count", "sku_id")
w_sku_id_distinct_ids_by_row_c_2 = Window.partitionBy("sku_id", "distinct_ids").orderBy("row_count", "sku_id").rowsBetween(-6, -1)

base_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws(": ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

per_group = (
    base_moves
    .groupBy("sku_id")
    .agg(
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

with_trends = (
    per_group
    .select(
        "*",
        (F.col("distinct_ids") - F.lag("distinct_ids").over(w_sku_id_distinct_ids_by_row_c)).alias("distinct_ids_change"),
        F.max("distinct_ids").over(w_sku_id_distinct_ids_by_row_c_2).alias("distinct_ids_max_6_prior"),
    )
)

result = (
    with_trends
    .filter(F.col("distinct_ids_change") > -3)
    .select(
        "sku_id",
        "move_type_list",
        "operator_list",
        "last_moved_at",
        "row_count",
        "distinct_ids_change",
    )
    .orderBy(F.col("row_count").desc(), "distinct_ids_change")
)
