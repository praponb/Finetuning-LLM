from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_id_by_days = Window.partitionBy("sku_id", "warehouse_id").orderBy("days_since_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")

prepared_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("operator") == "op_chan"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("qty").cast("string")).alias("move_type_ref"),
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

sequenced = (
    prepared_moves
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "days_since_moved_at",
        "move_type_ref",
        "operator_clean",
        F.first("days_since_moved_at").over(w_sku_id_warehouse_id_by_days).alias("first_days_since_moved_at"),
        F.count(F.lit(1)).over(w_warehouse_id_operator).alias("rows_in_warehouse_id_operator"),
    )
)

with_details_sku = (
    sequenced.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.size(F.col("sk.labels")) >= 1)
    .select(
        "s.id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.days_since_moved_at",
        "s.move_type_ref",
        "s.operator_clean",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
        "sk.sku_code",
    )
)

with_parent_sku = (
    with_details_sku.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

result = (
    with_parent_sku
    .rollup("sku_category", "operator")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.upper(F.trim(F.col("sku_code"))).like("A%"), 1)).alias("n_upper_trim_sku_code_like_a"),
    )
)
