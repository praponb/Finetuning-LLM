from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")) | (F.col("operator") == "op_bee"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
    )
)

qualified = (
    cleaned_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator") == "op_eve") & (F.col("st.qty") >= 116), "left_anti")
)

result = (
    qualified
    .cube("sku_id", "warehouse_id")
    .agg(
        F.sum("qty").alias("total_qty"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("sku_id").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
