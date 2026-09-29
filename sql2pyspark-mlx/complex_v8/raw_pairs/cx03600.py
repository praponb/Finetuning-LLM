from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter(F.col("move_type") == "OUTBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.concat_ws(" / ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

move_side = cleaned_moves.select("move_type")

move_side_2 = (
    df_stock_moves
    .filter((F.col("move_type") == "TRANSFER") & (F.col("qty") >= 111))
    .select("operator")
)

result = (
    move_side
    .subtract(move_side_2)
    .orderBy(F.col("move_type").desc())
)
