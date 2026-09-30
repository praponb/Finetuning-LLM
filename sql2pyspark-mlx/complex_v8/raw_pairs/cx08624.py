from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("moved_at").desc(), F.col("id").desc())

base = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

tagged = (
    base
    .filter(F.col("sku_id") >= 9)
    .select(
        "*",
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
    )
)

unique_rows = (
    tagged
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

stats = (
    unique_rows
    .filter(F.col("sku_id").isNotNull())
    .groupBy(
        "operator",
        F.date_trunc("month", "moved_at").alias("moved_at_month"),
        "sku_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .filter(F.col("cnt") >= 2)
)

per_label = (
    stats
    .select(
        "operator",
        "moved_at_month",
        "sku_id",
        "cnt",
        "distinct_warehouse_ids",
        "avg_unit_price",
        F.explode("operator_list").alias("operator_list_item"),
    )
)

exploded_detail_moves = (
    per_label.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.operator",
        "s.moved_at_month",
        "s.sku_id",
        "s.cnt",
        "s.distinct_warehouse_ids",
        "s.operator_list_item",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

result = (
    exploded_detail_moves
    .select(
        "operator",
        "sku_id",
        "cnt",
        "distinct_warehouse_ids",
        "operator_list_item",
        "move_id",
        "move_moved_at",
        "move_warehouse_id",
    )
    .orderBy(F.col("move_id").desc(), F.col("sku_id").desc())
)
