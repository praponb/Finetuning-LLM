from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_count = Window.partitionBy("move_count")

valid = (
    df_stock_moves
    .filter((F.hour(F.col("moved_at")).between(10, 12)) & (F.col("operator") == "op_chan"))
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.count("*").alias("move_count"),
    )
)

profiled = (
    valid.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

labeled = (
    profiled
    .filter(F.col("qty") < 372)
    .select(
        "id",
        "sku_id",
        "move_type",
        "unit_price",
        "move_count",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

sequenced = (
    labeled
    .withColumn("move_count_min_unit_price", F.min("unit_price").over(w_move_count))
    .withColumn("move_count_sum_unit_price", F.sum("unit_price").over(w_move_count))
)

result = (
    sequenced
    .filter(F.col("unit_price") < 387.1)
    .select(
        "id",
        "sku_id",
        "move_type",
        "unit_price",
        "move_count_min_unit_price",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
    .orderBy(F.col("move_count_min_unit_price").desc(), F.col("id").desc())
)
