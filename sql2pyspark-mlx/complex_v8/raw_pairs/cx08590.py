from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("move_type_or_unknown").orderBy("qty_bucket", "id")

cleaned_moves = (
    df_stock_moves
    .filter((F.col("unit_price") > 119.5) & (F.col("qty") < 393))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.when(F.col("operator").isin("op_dew", "op_ann"), "group_a").when(F.col("operator") == "op_chan", "group_b").otherwise("other").alias("operator_group"),
    )
)

enriched = (
    cleaned_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "move_type_or_unknown",
        "operator_len",
        (F.col("qty") - F.col("operator_len")).alias("qty_minus_operator_len"),
    )
)

labeled = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "move_type_or_unknown",
        "qty_minus_operator_len",
        (F.floor(F.col("operator_len") / 2) * 2).alias("operator_len_bucket"),
        F.least(F.col("qty_minus_operator_len"), F.col("operator_len")).alias("least_qty_minus_operator_len_operator_len"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

latest = (
    labeled
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .groupBy("operator_len_bucket", "move_type")
    .pivot("move_type_or_unknown", ["INBOUND", "UNKNOWN"])
    .agg(F.max("qty").alias("max_v"), F.count("qty").alias("n"))
    .orderBy("operator_len_bucket", "move_type")
)
