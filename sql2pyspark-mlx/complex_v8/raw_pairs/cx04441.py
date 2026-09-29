from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator", "move_type").orderBy(F.col("warehouse_opened_on").desc(), F.col("id").desc())
w_moved_at_month = Window.partitionBy(F.date_trunc("month", "moved_at"))
w_all_by_moved_at_running = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc(), "id")
w_dedup_2 = Window.partitionBy("sku_id").orderBy(F.col("days_since_prev_warehouse_opened_on").desc(), F.col("id").desc())

cleaned = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("unit_price") > 60.4))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

with_details_warehouse = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.least_unit_price_qty",
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

deduped = (
    with_details_warehouse
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    deduped
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "least_unit_price_qty",
        "warehouse_opened_on",
        F.round(F.avg("least_unit_price_qty").over(w_moved_at_month), 2).alias("moved_at_month_avg_least_unit_price_qty"),
        F.round(F.col("least_unit_price_qty") - F.avg("least_unit_price_qty").over(w_moved_at_month), 2).alias("least_unit_price_qty_vs_moved_at_month_avg"),
        F.first("moved_at", ignorenulls=True).over(w_all_by_moved_at_running).alias("first_known_moved_at"),
        F.lead("moved_at", 3).over(w_all_by_moved_at).alias("next3_moved_at"),
        F.datediff(F.col("warehouse_opened_on"), F.lag("warehouse_opened_on").over(w_all_by_moved_at)).alias("days_since_prev_warehouse_opened_on"),
        F.max("least_unit_price_qty").over(w_moved_at_month).alias("moved_at_month_max_least_unit_price_qty"),
    )
)

enriched = (
    metrics.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (~F.col("sk.is_active")), "inner")
    .filter(F.col("sk.unit_cost") <= 330.6)
    .select(
        "s.id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.least_unit_price_qty_vs_moved_at_month_avg",
        "s.first_known_moved_at",
        "s.next3_moved_at",
        "s.days_since_prev_warehouse_opened_on",
        "s.moved_at_month_max_least_unit_price_qty",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

deduped_2 = (
    enriched
    .withColumn("row_num", F.row_number().over(w_dedup_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined_sku = (
    deduped_2.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.unit_cost") <= 277.0), "inner")
    .select("s.*", F.col("sk.unit_cost").alias("sku_unit_cost_2"))
)

result = (
    joined_sku
    .select(
        "id",
        F.col("sku_unit_cost_2").alias("sku_unit_cost_2_v"),
        F.col("sku_unit_cost").alias("sku_unit_cost_v"),
    )
    .unpivot(
        ids=["id"],
        values=["sku_unit_cost_2_v", "sku_unit_cost_v"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
