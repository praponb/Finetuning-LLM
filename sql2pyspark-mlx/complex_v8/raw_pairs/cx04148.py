from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("category").orderBy(F.col("weight_kg").desc(), F.col("id").desc())

prepared = (
    df_skus
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
    )
)

latest_per_key = (
    prepared
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

with_counts = (
    latest_per_key.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

move_stats_2 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("operator").isin("op_chan", "op_eve", "op_dew"), 1)).alias("move_operator_count"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

profiled = (
    with_counts.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        "st.last_moved_at",
    )
)

move_stats_3 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.max("moved_at").alias("last_moved_at_2"),
        F.max("moved_at").alias("last_moved_at_3"),
    )
)

with_counts_2 = (
    profiled.alias("s")
    .join(move_stats_3.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select("s.*", "st.last_moved_at_2", "st.last_moved_at_3")
)

result = (
    with_counts_2
    .select("id", "move_operator_count", "move_count")
    .unpivot(
        ids=["id"],
        values=["move_operator_count", "move_count"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
