from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator")

base_moves = (
    df_stock_moves
    .filter(F.col("unit_price") > 125.9)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
        F.when(F.col("move_type").isin("INBOUND", "ADJUST"), "group_a").when(F.col("move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.round(F.col("unit_price") * 1.1, 2).alias("unit_price_adj"),
    )
)

ranked = (
    base_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price_adj",
        F.min("unit_price_adj").over(w_operator).alias("operator_min_unit_price_adj"),
    )
)

result = (
    ranked
    .rollup("move_type", "sku_id")
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
    )
    .orderBy(F.col("move_type").asc_nulls_last(), F.col("sku_id").asc_nulls_last())
)
