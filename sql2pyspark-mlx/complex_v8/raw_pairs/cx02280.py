from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_ties = Window.orderBy("qty")
w_all_by_qty_full = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty = Window.orderBy(F.col("qty").asc_nulls_last(), "id")
w_all_by_qty_full_2 = Window.orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_qty_decile = Window.partitionBy("qty_decile")
w_qty_decile_by_second_qty = Window.partitionBy("qty_decile").orderBy("second_qty", "id")

scoped = (
    df_stock_moves
    .filter((F.col("unit_price").between(119.5, 372.6)) | (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
    )
)

scored = (
    scoped
    .filter(F.hour(F.col("moved_at")).between(10, 13))
    .select("id", "qty", F.upper(F.trim(F.col("operator"))).alias("operator_clean"))
)

ranked = (
    scored
    .withColumn("qty_cume_dist", F.round(F.cume_dist().over(w_all_by_qty_ties), 3))
    .withColumn("second_qty", F.nth_value("qty", 2).over(w_all_by_qty_full))
)

with_windows = (
    ranked
    .select(
        "*",
        F.ntile(10).over(w_all_by_qty).alias("qty_decile"),
        F.nth_value("qty_cume_dist", 2).over(w_all_by_qty_full_2).alias("second_qty_cume_dist"),
    )
)

with_trends = (
    with_windows
    .select(
        "*",
        F.count(F.lit(1)).over(w_qty_decile).alias("rows_in_qty_decile"),
        F.row_number().over(w_qty_decile_by_second_qty).alias("second_qty_seq"),
    )
)

scored_2 = (
    with_trends
    .filter(F.col("qty") < 426)
    .select(
        "id",
        "qty",
        "qty_cume_dist",
        "second_qty",
        "qty_decile",
        "rows_in_qty_decile",
        F.round(F.col("second_qty") / F.nullif(F.col("rows_in_qty_decile"), F.lit(0)), 3).alias("second_qty_per_rows_in_qty_decile"),
        F.when(F.col("second_qty_cume_dist") >= 0.7, "large").when(F.col("second_qty_cume_dist") >= 0.2, "medium").otherwise("small").alias("second_qty_cume_dist_tier"),
    )
)

result = (
    scored_2
    .select(
        "id",
        "rows_in_qty_decile",
        "qty_decile",
        "qty",
        "second_qty",
    )
    .unpivot(
        ids=["id"],
        values=["rows_in_qty_decile", "qty_decile", "qty", "second_qty"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
