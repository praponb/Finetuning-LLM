from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_is_active_by_unit_c = Window.partitionBy("category", "is_active").orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_category_is_active = Window.partitionBy("category", "is_active")
w_category_is_active_by_unit_c_2 = Window.partitionBy("category", "is_active").orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_is_active_by_unit_c_3 = Window.partitionBy("category", "is_active").orderBy("unit_cost", "id")

cleaned_skus = (
    df_skus
    .filter((F.col("is_active")) & (~F.col("category").isin("Food", "Electronics")))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
    )
)

with_windows = (
    cleaned_skus
    .select(
        "*",
        F.first("unit_cost", ignorenulls=True).over(w_category_is_active_by_unit_c).alias("first_known_unit_cost"),
        F.count(F.lit(1)).over(w_category_is_active).alias("rows_in_category_is_active"),
        F.round(F.col("unit_cost") - F.avg("unit_cost").over(w_category_is_active), 2).alias("unit_cost_vs_category_is_active_avg"),
        F.round(F.avg("unit_cost").over(w_category_is_active), 2).alias("category_is_active_avg_unit_cost"),
        F.last("unit_cost").over(w_category_is_active_by_unit_c_2).alias("last_unit_cost"),
        F.dense_rank().over(w_category_is_active_by_unit_c_3).alias("unit_cost_dense_rank"),
    )
)

exploded_detail_moves = (
    with_windows.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.category",
        "s.unit_cost",
        "s.labels",
        "s.is_active",
        "s.rows_in_category_is_active",
        "s.category_is_active_avg_unit_cost",
        "s.last_unit_cost",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

result = (
    exploded_detail_moves
    .select(
        "id",
        "move_id",
        "rows_in_category_is_active",
        "move_qty",
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["rows_in_category_is_active", "move_qty"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
