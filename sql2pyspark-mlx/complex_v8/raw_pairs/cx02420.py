from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("sku_id").orderBy(F.col("qty_bucket").desc(), F.col("id").desc())

scoped = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix_2"),
    )
)

screened = (
    scoped.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity") < 11506) & (F.lower(F.col("w.wh_code")).like("%nx%")), "left_anti")
)

filtered = (
    screened
    .crossJoin(screened.agg(F.avg("qty").alias("overall_avg_qty")))
    .filter(F.col("qty") > F.col("overall_avg_qty"))
    .drop("overall_avg_qty")
)

labeled = (
    filtered
    .filter(~F.col("move_type").isin("OUTBOUND", "TRANSFER"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "move_type_prefix",
        "qty_bucket",
        F.coalesce(F.col("qty_bucket"), F.lit(0)).alias("qty_bucket_filled"),
        F.abs(F.col("qty_bucket") - 325).alias("qty_bucket_distance"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket_2"),
    )
)

combined_details_warehouse = (
    labeled.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select("s.*", F.col("w.opened_on").alias("warehouse_opened_on"))
)

latest_per_key = (
    combined_details_warehouse
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .filter(F.col("qty_bucket") <= 300)
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "move_type_prefix",
        "qty_bucket_filled",
        "qty_bucket_distance",
        "qty_bucket_2",
        F.concat_ws(": ", F.col("move_type_prefix"), F.col("operator")).alias("move_type_prefix_operator_label"),
    )
    .orderBy(F.col("qty_bucket_distance").desc_nulls_first())
)
