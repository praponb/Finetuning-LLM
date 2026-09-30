from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_id").orderBy(F.col("prior_rows_7d").desc(), F.col("id").desc())
w_all_by_sku_weight_kg_full = Window.orderBy("sku_weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_sku_unit_cost = Window.orderBy(F.col("sku_unit_cost").asc_nulls_last(), "id")
w_all_by_sku_weight_kg_running = Window.orderBy("sku_weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_prior_rows_7d = Window.partitionBy("prior_rows_7d")

covered = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.qty").alias("prior_qty_7d"),
    )
)

latest = (
    covered
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

combined_details_sku = (
    latest.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_rows_7d",
        "s.prior_qty_7d",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

windowed = (
    combined_details_sku
    .select(
        "*",
        F.first("prior_qty_7d").over(w_all_by_sku_weight_kg_full).alias("first_prior_qty_7d"),
        F.row_number().over(w_all_by_sku_unit_cost).alias("rn"),
        F.min("sku_weight_kg").over(w_all_by_sku_weight_kg_running).alias("min_sku_weight_kg_so_far"),
        F.count(F.lit(1)).over(w_prior_rows_7d).alias("rows_in_prior_rows_7d"),
        F.lag("sku_weight_kg").over(w_all_by_sku_unit_cost).alias("prev_sku_weight_kg"),
    )
)

result = (
    windowed
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_rows_7d",
        "prior_qty_7d",
        "sku_weight_kg",
        "sku_unit_cost",
        "sku_labels",
        "first_prior_qty_7d",
        "min_sku_weight_kg_so_far",
        "rows_in_prior_rows_7d",
        "prev_sku_weight_kg",
        F.array_contains(F.col("sku_labels"), "hazmat").alias("has_hazmat"),
    )
    .orderBy("prev_sku_weight_kg", "first_prior_qty_7d")
)
