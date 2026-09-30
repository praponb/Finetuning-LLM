from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter((F.col("operator") != "op_eve") & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.when(F.col("qty") >= 288, "A").when(F.col("qty") >= 124, "B").otherwise("C").alias("qty_tier"),
    )
)

filtered_2 = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.to_date(F.col("st.moved_at")) < F.lit("2025-02-24").cast("date")), "left_anti")
)

result = (
    filtered_2
    .groupingSets(
        [["move_type", "operator", "warehouse_id"], ["move_type"], []],
        "move_type", "operator", "warehouse_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.stddev("operator_len"), 2).alias("stddev_operator_len"),
    )
    .select(
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        F.coalesce(F.col("operator"), F.lit("ALL")).alias("operator"),
        "warehouse_id",
        "gid",
        "stddev_operator_len",
    )
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("operator").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
