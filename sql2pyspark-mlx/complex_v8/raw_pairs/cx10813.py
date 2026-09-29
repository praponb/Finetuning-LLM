from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_3d_sku_id = Window.partitionBy("prior_rows_3d", "sku_id")
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), "id")
w_prior_rows_3d_by_moved_at_mo = Window.partitionBy("prior_rows_3d").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-2, Window.currentRow)

matched = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.unit_price").alias("prior_unit_price_3d"),
    )
)

with_windows = (
    matched
    .select(
        "id",
        "sku_id",
        "moved_at",
        "prior_unit_price_3d",
        F.count(F.lit(1)).over(w_prior_rows_3d_sku_id).alias("rows_in_prior_rows_3d_sku_id"),
        F.ntile(3).over(w_sku_id_by_moved_at).alias("moved_at_tercile"),
        F.round(F.avg("prior_unit_price_3d").over(w_prior_rows_3d_by_moved_at_mo), 2).alias("prior_unit_price_3d_avg_3"),
    )
)

combined_details_sku = (
    with_windows.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.sku_id",
        "s.rows_in_prior_rows_3d_sku_id",
        F.col("sk.labels").alias("sku_labels"),
        "sk.sku_code",
    )
)

qualified = (
    combined_details_sku.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (~F.col("sk.is_active")), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "sku_id",
        "sku_labels",
        "sku_code",
    )
    .orderBy(F.col("sku_id").desc(), F.col("sku_code").desc())
)
