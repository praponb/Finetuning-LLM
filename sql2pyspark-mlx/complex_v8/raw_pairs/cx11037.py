from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_move_id_by_greates = Window.partitionBy("move_type", "move_id").orderBy(F.col("greatest_weight_kg_unit_cost").desc(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_len = Window.partitionBy("category_len")
w_move_type_move_id_by_greates_2 = Window.partitionBy("move_type", "move_id").orderBy(F.col("greatest_weight_kg_unit_cost").desc(), "id", "move_id")

scoped_skus = (
    df_skus
    .filter((~F.col("is_active")) & (F.col("sku_code").isNotNull()))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        F.greatest(F.col("weight_kg"), F.col("unit_cost")).alias("greatest_weight_kg_unit_cost"),
    )
)

detail_rows = (
    scoped_skus.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.category",
        "s.greatest_weight_kg_unit_cost",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        "st.move_type",
        F.col("st.qty").alias("move_qty"),
    )
)

enriched = detail_rows.select("*", F.length(F.trim(F.col("category"))).alias("category_len"))

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("move_count"),
    )
)

with_counts = (
    enriched.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

metrics = (
    with_counts
    .select(
        "id",
        "category",
        "greatest_weight_kg_unit_cost",
        "move_id",
        "move_unit_price",
        "category_len",
        "last_moved_at",
        "move_count",
        F.last("move_qty").over(w_move_type_move_id_by_greates).alias("last_move_qty"),
        F.count(F.lit(1)).over(w_category_len).alias("rows_in_category_len"),
        F.rank().over(w_move_type_move_id_by_greates_2).alias("greatest_weight_kg_unit_cost_rank"),
        F.size(F.collect_set("move_id").over(w_category_len)).alias("distinct_move_id_in_category_len"),
        F.nth_value("move_unit_price", 2).over(w_move_type_move_id_by_greates).alias("second_move_unit_price"),
        F.round(F.avg("category_len").over(w_category_len), 2).alias("category_len_avg_category_len"),
    )
)

result = (
    metrics
    .select(
        "id",
        "greatest_weight_kg_unit_cost",
        "move_id",
        "move_unit_price",
        "category_len",
        "move_count",
        "last_move_qty",
        "rows_in_category_len",
        "greatest_weight_kg_unit_cost_rank",
    )
    .orderBy(F.col("id").desc(), F.col("last_move_qty").desc(), "move_id")
    .limit(10)
)
