from pyspark.sql import functions as F

filtered = (
    df_warehouses
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.when(F.col("capacity") >= 11506, "large").when(F.col("capacity") >= 4912, "medium").otherwise("small").alias("capacity_tier"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.max("moved_at").alias("last_moved_at"))
)

with_counts = (
    filtered.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select("s.*", "st.last_moved_at")
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at_2"),
        F.count("*").alias("move_count"),
        F.count("*").alias("move_count_2"),
    )
)

summarized_rows = (
    with_counts.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.last_moved_at_2",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

result = (
    summarized_rows
    .select("id", "move_count_2", "capacity")
    .unpivot(
        ids=["id"],
        values=["move_count_2", "capacity"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "measure")
)
