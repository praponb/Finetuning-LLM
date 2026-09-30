from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("unit_price").desc(), F.col("id").desc())

prepared_moves = (
    df_stock_moves
    .filter((F.col("move_type") == "TRANSFER") & (~F.col("operator").isin("op_chan", "op_ann")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

deduped = (
    prepared_moves
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("move_type") == "TRANSFER", 1)).alias("move_move_type_count"),
        F.count(F.when(F.col("move_type") == "OUTBOUND", 1)).alias("move_move_type_count_2"),
    )
)

profiled = (
    deduped.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
        F.coalesce(F.col("st.move_move_type_count_2"), F.lit(0)).alias("move_move_type_count_2"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"))
)

summarized_rows = (
    profiled.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

result = (
    summarized_rows
    .groupBy("move_move_type_count_2", "move_move_type_count")
    .pivot("operator", ["op_chan", "op_eve", "op_ann", "op_dew"])
    .agg(F.max("unit_price").alias("max_v"), F.count("unit_price").alias("n"))
    .orderBy("move_move_type_count_2", "move_move_type_count")
)
