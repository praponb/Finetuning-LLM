from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

tagged = (
    cleaned_moves
    .filter(F.col("unit_price") <= 293.2)
    .withColumn("operator_move_type_slug_label", F.concat_ws(" / ", F.col("operator"), F.col("move_type_slug")))
)

result = (
    tagged
    .filter(F.col("operator") == "op_bee")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "unit_price",
        "move_type_slug",
        "operator_move_type_slug_label",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug_2"),
    )
    .orderBy(F.col("sku_id").desc())
)
