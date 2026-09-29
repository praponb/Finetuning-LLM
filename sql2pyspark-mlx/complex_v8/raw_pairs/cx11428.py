from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").asc_nulls_last())
w_moved_at_date_year_warehouse = Window.partitionBy(F.year("moved_at_date"), "warehouse_id")
w_all_by_qty_prior_6 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-6, -1)

base_moves = df_stock_moves.filter(F.col("unit_price") > 180.0)

tagged = (
    base_moves
    .filter(F.col("sku_id") >= 12)
    .select(
        "*",
        F.when(F.col("unit_price") >= 367.7, "large").when(F.col("unit_price") >= 161.4, "medium").otherwise("small").alias("unit_price_tier"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.abs(F.col("qty") - 198).alias("qty_distance"),
    )
)

ranked = (
    tagged
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_unit_price_ties), 3).alias("unit_price_cume_dist"),
        F.round(F.col("qty_distance") - F.avg("qty_distance").over(w_moved_at_date_year_warehouse), 2).alias("qty_distance_vs_moved_at_date_year_warehouse_id_"),
        F.count(F.lit(1)).over(w_moved_at_date_year_warehouse).alias("rows_in_moved_at_date_year_warehouse_id"),
        F.max("unit_price").over(w_all_by_qty_prior_6).alias("unit_price_max_6_prior"),
    )
)

result = (
    ranked
    .select(
        "id",
        "unit_price_cume_dist",
        "unit_price_max_6_prior",
        "unit_price",
    )
    .unpivot(
        ids=["id"],
        values=["unit_price_cume_dist", "unit_price_max_6_prior", "unit_price"],
        variableColumnName="field",
        valueColumnName="value",
    )
)
