from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_id_operator_by_moved_at = Window.partitionBy("move_id", "operator").orderBy(F.col("moved_at_year").desc(), "id", "move_id")
w_move_id_by_move_unit_price = Window.partitionBy("move_id").orderBy(F.col("move_unit_price").desc(), "id", "move_id")
w_sku_id = Window.partitionBy("sku_id")
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_operator_by_unit_price_ties = Window.partitionBy("operator").orderBy("unit_price")
w_sku_id_by_sku_id_max_unit_pr = Window.partitionBy("sku_id").orderBy(F.col("sku_id_max_unit_price").desc_nulls_first(), "id", "move_id").rowsBetween(-3, -1)

cleaned_moves = (
    df_stock_moves
    .filter((F.col("operator").isNotNull()) | (F.col("qty") < 387))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.year(F.col("moved_at")).alias("moved_at_year"),
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
    )
)

line_level = (
    cleaned_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.moved_at_year",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.unit_price").alias("move_unit_price"),
        "st.move_type",
    )
)

combined_details = (
    line_level.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        "s.moved_at_year",
        "s.move_id",
        "s.move_qty",
        "s.move_unit_price",
        "s.move_type",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

with_trends = (
    combined_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "unit_price",
        "move_id",
        "move_unit_price",
        "move_type",
        "warehouse_capacity",
        F.round(F.cume_dist().over(w_move_id_operator_by_moved_at), 3).alias("moved_at_year_cume_dist"),
        (F.col("move_qty") - F.lag("move_qty").over(w_move_id_by_move_unit_price)).alias("move_qty_change"),
        F.max("unit_price").over(w_sku_id).alias("sku_id_max_unit_price"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.round(F.col("move_unit_price") / F.nullif(F.sum("move_unit_price").over(w_sku_id_operator), F.lit(0)), 4).alias("move_unit_price_share_of_sku_id_operator"),
        F.dense_rank().over(w_operator_by_unit_price_ties).alias("unit_price_dense_rank"),
        F.count(F.lit(1)).over(w_sku_id_operator).alias("rows_in_sku_id_operator"),
        F.round(F.avg("moved_at_year_cume_dist").over(w_sku_id_by_sku_id_max_unit_pr), 2).alias("moved_at_year_cume_dist_avg_3_prior"),
    )
)

screened = (
    ranked
    .crossJoin(ranked.agg(F.avg("sku_id_max_unit_price").alias("overall_avg_sku_id_max_unit_price")))
    .filter(F.col("sku_id_max_unit_price") > F.col("overall_avg_sku_id_max_unit_price"))
    .drop("overall_avg_sku_id_max_unit_price")
)

result = (
    screened
    .filter(F.col("move_unit_price") < 399.6)
    .select(
        "id",
        "sku_id",
        "operator",
        "move_id",
        "move_unit_price",
        "move_type",
        "warehouse_capacity",
        "moved_at_year_cume_dist",
        "move_qty_change",
        "unit_price_dense_rank",
        "rows_in_sku_id_operator",
        "moved_at_year_cume_dist_avg_3_prior",
        F.when(F.col("rows_in_sku_id_operator") >= 13, "large").when(F.col("rows_in_sku_id_operator") >= 8, "medium").otherwise("small").alias("rows_in_sku_id_operator_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
    .orderBy(F.col("move_qty_change").desc(), F.col("operator").asc())
)
