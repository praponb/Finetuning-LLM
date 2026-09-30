from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("warehouse_id").orderBy(F.col("qty_bucket").desc(), F.col("id").desc())

prepared_moves = (
    df_stock_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.initcap(F.trim(F.col("operator"))).alias("operator_clean"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.hour(F.col("moved_at")).alias("moved_at_hour"),
    )
)

deduped = (
    prepared_moves
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_details_warehouse = (
    deduped.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.labels").alias("sku_labels"),
        "sk.sku_code",
    )
)

totals = (
    with_details_warehouse
    .groupBy("moved_at_hour", "move_type", "operator")
    .agg(
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.sum("moved_at_hour").alias("total_moved_at_hour"),
        F.count("*").alias("n_rows"),
        F.round(F.avg("qty_bucket"), 2).alias("avg_qty_bucket"),
        F.round(F.sum("moved_at_hour") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("moved_at_hour_to_qty_ratio"),
    )
)

screened = (
    totals
    .crossJoin(totals.agg(F.avg("n_rows").alias("overall_avg_n_rows")))
    .filter(F.col("n_rows") > F.col("overall_avg_n_rows"))
    .drop("overall_avg_n_rows")
)

derived = screened.withColumn("operator_len", F.length(F.trim(F.col("operator"))))

result = (
    derived
    .groupBy("n_rows", "moved_at_hour")
    .pivot("move_type", ["ADJUST", "INBOUND", "TRANSFER", "OUTBOUND"])
    .agg(F.avg("moved_at_hour_to_qty_ratio").alias("avg_v"), F.count("moved_at_hour_to_qty_ratio").alias("n"))
)
