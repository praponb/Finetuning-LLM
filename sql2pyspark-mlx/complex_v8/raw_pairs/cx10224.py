from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("operator", "sku_id").orderBy("warehouse_capacity", "id")

recent = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.initcap(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.when(F.col("unit_price") >= 289.7, "HIGH").when(F.col("unit_price") >= 205.1, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
    )
)

with_details_warehouse = (
    recent.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.move_type_clean",
        "s.unit_price_tier",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

latest_per_key = (
    with_details_warehouse
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enriched = (
    latest_per_key
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("warehouse_wh_code")), " ", "_")).alias("warehouse_wh_code_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("unit_price_tier")), " ", "_")).alias("unit_price_tier_slug"),
        F.dayofweek(F.col("warehouse_opened_on")).alias("warehouse_opened_on_dayofweek"),
    )
)

result = (
    enriched
    .rollup("move_type", "operator")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("warehouse_capacity") / F.nullif(F.sum("warehouse_opened_on_dayofweek"), F.lit(0)), 3).alias("warehouse_capacity_to_warehouse_opened_on_dayofw"),
        F.round(F.sum("warehouse_opened_on_dayofweek") / F.nullif(F.sum("warehouse_capacity"), F.lit(0)), 3).alias("warehouse_opened_on_dayofweek_to_warehouse_capac"),
        F.sum("warehouse_opened_on_dayofweek").alias("total_warehouse_opened_on_dayofweek"),
    )
    .orderBy(F.col("move_type").asc_nulls_last(), F.col("operator").asc_nulls_last())
)
