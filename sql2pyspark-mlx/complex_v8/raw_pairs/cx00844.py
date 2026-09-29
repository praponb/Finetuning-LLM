from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_moved_at_day_ties = Window.partitionBy("sku_id").orderBy("moved_at_day")
w_sku_id_by_moved_at_day_prior = Window.partitionBy("sku_id").orderBy("moved_at_day", "id").rowsBetween(-1, -1)

cleaned_moves = (
    df_stock_moves
    .filter((F.col("qty") >= 145) & (F.col("warehouse_id") >= 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

windowed = (
    cleaned_moves
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "least_unit_price_qty",
        F.dense_rank().over(w_sku_id_by_moved_at_day_ties).alias("moved_at_day_dense_rank"),
        F.round(F.avg("least_unit_price_qty").over(w_sku_id_by_moved_at_day_prior), 2).alias("least_unit_price_qty_avg_1_prior"),
    )
)

scored = (
    windowed
    .filter(F.col("least_unit_price_qty_avg_1_prior") < 375.2)
    .select(
        "*",
        F.round(F.col("least_unit_price_qty") / F.nullif(F.col("moved_at_day_dense_rank"), F.lit(0)), 3).alias("least_unit_price_qty_per_moved_at_day_dense_rank"),
        F.abs(F.col("least_unit_price_qty") - 350.3).alias("least_unit_price_qty_distance"),
        F.when(F.col("least_unit_price_qty") >= 375.2, "large").when(F.col("least_unit_price_qty") >= 125.8, "medium").otherwise("small").alias("least_unit_price_qty_tier"),
    )
)

screened = (
    scored
    .crossJoin(scored.agg(F.min("least_unit_price_qty_distance").alias("overall_min_least_unit_price_qty_distance")))
    .filter(F.col("least_unit_price_qty_distance") > F.col("overall_min_least_unit_price_qty_distance"))
    .drop("overall_min_least_unit_price_qty_distance")
)

result = (
    screened
    .select("id", "least_unit_price_qty_distance", "least_unit_price_qty")
    .unpivot(
        ids=["id"],
        values=["least_unit_price_qty_distance", "least_unit_price_qty"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)
