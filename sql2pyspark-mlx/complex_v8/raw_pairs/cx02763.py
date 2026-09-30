from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("qty").desc(), F.col("id").desc(), F.col("move_id").desc())
w_move_operator = Window.partitionBy("move_operator")
w_warehouse_id_move_type_by_mo = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("move_moved_at").desc(), "id", "move_id")

base_moves = (
    df_stock_moves
    .filter(F.col("qty") > 99)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)

detail_rows_moves = (
    base_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

qualified = (
    detail_rows_moves
    .crossJoin(detail_rows_moves.agg(F.max("move_unit_price").alias("overall_max_move_unit_price")))
    .filter(F.col("move_unit_price") < F.col("overall_max_move_unit_price"))
    .drop("overall_max_move_unit_price")
)

deduped = (
    qualified
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    deduped
    .select(
        "*",
        F.max("move_unit_price").over(w_move_operator).alias("move_operator_max_move_unit_price"),
        F.ntile(4).over(w_warehouse_id_move_type_by_mo).alias("move_moved_at_quartile"),
        F.lead("move_moved_at").over(w_warehouse_id_move_type_by_mo).alias("next_move_moved_at"),
    )
)

tagged = (
    ranked
    .select(
        "*",
        (F.col("move_unit_price") + F.col("qty")).alias("move_unit_price_plus_qty"),
        F.round(F.col("move_moved_at_quartile") / F.nullif(F.col("move_unit_price"), F.lit(0)), 3).alias("move_moved_at_quartile_per_move_unit_price"),
    )
)

result = (
    tagged
    .select(
        "id",
        "move_id",
        "move_unit_price_plus_qty",
        "move_moved_at_quartile_per_move_unit_price",
        "move_unit_price",
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["move_unit_price_plus_qty", "move_moved_at_quartile_per_move_unit_price", "move_unit_price"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
