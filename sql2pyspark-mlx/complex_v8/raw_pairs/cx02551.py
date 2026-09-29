from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category = Window.partitionBy("category")
w_category_by_unit_cost_bucket = Window.partitionBy("category").orderBy("unit_cost_bucket")

prepared_skus = (
    df_skus
    .filter((F.col("is_active")) & (F.col("unit_cost") >= 60.9))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        (F.floor(F.col("unit_cost") / 50) * 50).alias("unit_cost_bucket"),
    )
)

with_windows = (
    prepared_skus
    .select(
        "id",
        "unit_cost",
        "unit_cost_bucket",
        F.min("unit_cost_bucket").over(w_category).alias("category_min_unit_cost_bucket"),
        F.round(F.cume_dist().over(w_category_by_unit_cost_bucket), 3).alias("unit_cost_bucket_cume_dist"),
        F.count(F.lit(1)).over(w_category).alias("rows_in_category"),
    )
)

with_children = (
    with_windows.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.hour(F.col("st.moved_at")).between(9, 12)), "inner")
    .select(
        "s.id",
        "s.unit_cost",
        "s.category_min_unit_cost_bucket",
        "s.unit_cost_bucket_cume_dist",
        "s.rows_in_category",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

result = (
    with_children
    .select(
        "id",
        "unit_cost",
        "category_min_unit_cost_bucket",
        "unit_cost_bucket_cume_dist",
        "rows_in_category",
        "move_id",
        "move_qty",
        (F.floor(F.col("unit_cost") / 50) * 50).alias("unit_cost_bucket"),
        (F.col("rows_in_category") + F.col("move_qty")).alias("rows_in_category_plus_move_qty"),
    )
    .orderBy("unit_cost", F.col("id").desc())
)
