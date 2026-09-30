from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_warehouse_id_by_wa = Window.partitionBy("move_type", "warehouse_id").orderBy("warehouse_opened_on", "id")
w_n_rows_next_warehouse_opened = Window.partitionBy("n_rows", "next_warehouse_opened_on_year")
w_distinct_ids_next_warehouse = Window.partitionBy("distinct_ids", "next_warehouse_opened_on_year").orderBy(F.col("next_warehouse_opened_on_span_days").asc_nulls_last(), "next_warehouse_opened_on_year")

prepared = (
    df_stock_moves
    .filter((F.col("qty") < 384) & (F.col("operator").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.date_trunc("quarter", F.col("moved_at")).alias("moved_at_quarter"),
    )
)

labeled = (
    prepared
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        F.concat(F.col("move_type"), F.lit("#"), F.col("id").cast("string")).alias("move_type_ref"),
    )
)

filtered = (
    labeled.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") == "TRANSFER") & (~F.col("st.operator").isin("op_chan", "op_bee")), "left_anti")
)

combined_details = (
    filtered.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.opened_on").between(F.lit("2024-11-25").cast("date"), F.lit("2025-12-17").cast("date"))), "inner")
    .filter(F.col("w.opened_on").isNotNull())
    .select(
        "s.id",
        "s.warehouse_id",
        "s.move_type",
        "s.move_type_ref",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

sequenced = (
    combined_details
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "move_type_ref",
        "warehouse_city",
        "warehouse_opened_on",
        F.lead("warehouse_opened_on").over(w_move_type_warehouse_id_by_wa).alias("next_warehouse_opened_on"),
    )
)

totals = (
    sequenced
    .groupBy(
        F.year("next_warehouse_opened_on").alias("next_warehouse_opened_on_year"),
    )
    .agg(
        F.datediff(F.max("next_warehouse_opened_on"), F.min("next_warehouse_opened_on")).alias("next_warehouse_opened_on_span_days"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("warehouse_opened_on").alias("last_warehouse_opened_on"),
        F.count("*").alias("n_rows"),
    )
)

metrics = (
    totals
    .select(
        "next_warehouse_opened_on_year",
        "next_warehouse_opened_on_span_days",
        "distinct_ids",
        "last_warehouse_opened_on",
        "n_rows",
        F.count(F.lit(1)).over(w_n_rows_next_warehouse_opened).alias("rows_in_n_rows_next_warehouse_opened_on_year"),
        F.round(F.col("distinct_ids") / F.nullif(F.sum("distinct_ids").over(w_n_rows_next_warehouse_opened), F.lit(0)), 4).alias("distinct_ids_share_of_n_rows_next_warehouse_open"),
        F.dense_rank().over(w_distinct_ids_next_warehouse).alias("next_warehouse_opened_on_span_days_dense_rank"),
    )
)

result = (
    metrics
    .filter(F.col("distinct_ids_share_of_n_rows_next_warehouse_open").between(0.1, 0.8))
    .select(
        "next_warehouse_opened_on_year",
        "next_warehouse_opened_on_span_days",
        "rows_in_n_rows_next_warehouse_opened_on_year",
        F.round(F.col("distinct_ids") / F.nullif(F.col("n_rows"), F.lit(0)), 3).alias("distinct_ids_per_n_rows"),
    )
    .orderBy(F.col("distinct_ids_per_n_rows").asc(), "next_warehouse_opened_on_year")
    .limit(10)
)
