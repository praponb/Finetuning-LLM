from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_all_by_avg_unit_price_full = Window.orderBy(F.col("avg_unit_price").desc(), "move_type", "warehouse_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_warehouse_id_by_av = Window.partitionBy("move_type", "warehouse_id").orderBy("avg_unit_price", "move_type", "warehouse_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_move_type_by_mo = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("move_type_warehouse_id_min_avg_unit_price").desc(), "move_type", "warehouse_id")
w_warehouse_id_move_type_by_mo_2 = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("move_type_warehouse_id_min_avg_unit_price").desc(), "move_type", "warehouse_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_avg_unit_price_runnin = Window.orderBy(F.col("avg_unit_price").desc(), "move_type", "warehouse_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_move_type_by_mo_3 = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("move_type_warehouse_id_min_avg_unit_price").asc_nulls_last(), "move_type", "warehouse_id")
w_all_by_avg_unit_price = Window.orderBy(F.col("avg_unit_price").desc(), "move_type", "warehouse_id")

valid_moves = (
    df_stock_moves
    .filter((F.col("move_type") == "TRANSFER") & (F.col("warehouse_id") <= 4))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
    )
)

per_group = (
    valid_moves
    .groupBy("move_type", "warehouse_id")
    .agg(
        F.sum(F.when(F.col("operator").isNotNull(), F.col("unit_price"))).alias("unit_price_operator_is_not_null"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

sequenced = (
    per_group
    .select(
        "move_type",
        "warehouse_id",
        "avg_unit_price",
        F.count(F.lit(1)).over(w_move_type_warehouse_id).alias("rows_in_move_type_warehouse_id"),
        F.min("avg_unit_price").over(w_move_type_warehouse_id).alias("move_type_warehouse_id_min_avg_unit_price"),
        F.nth_value("avg_unit_price", 2).over(w_all_by_avg_unit_price_full).alias("second_avg_unit_price"),
        F.round(F.col("avg_unit_price") / F.nullif(F.sum("avg_unit_price").over(w_move_type_warehouse_id), F.lit(0)), 4).alias("avg_unit_price_share_of_move_type_warehouse_id"),
        F.min("total_unit_price").over(w_move_type_warehouse_id_by_av).alias("min_total_unit_price_so_far"),
        F.round(F.col("total_unit_price") - F.avg("total_unit_price").over(w_move_type_warehouse_id), 2).alias("total_unit_price_vs_move_type_warehouse_id_avg"),
    )
)

with_activity = (
    sequenced
    .crossJoin(sequenced.agg(F.avg("avg_unit_price").alias("overall_avg_avg_unit_price")))
    .filter(F.col("avg_unit_price") > F.col("overall_avg_avg_unit_price"))
    .drop("overall_avg_avg_unit_price")
)

combined_details_warehouse = (
    with_activity.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity") <= 11506), "inner")
    .select(
        "s.move_type",
        "s.warehouse_id",
        "s.avg_unit_price",
        "s.move_type_warehouse_id_min_avg_unit_price",
        "s.min_total_unit_price_so_far",
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

tagged = (
    combined_details_warehouse
    .filter(F.col("min_total_unit_price_so_far") > 10500.6)
    .select(
        "move_type",
        "warehouse_id",
        "avg_unit_price",
        "move_type_warehouse_id_min_avg_unit_price",
        "min_total_unit_price_so_far",
        F.least(F.col("avg_unit_price"), F.col("warehouse_capacity")).alias("least_avg_unit_price_warehouse_capacity"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

sequenced_2 = (
    tagged
    .select(
        "move_type",
        "warehouse_id",
        "move_type_warehouse_id_min_avg_unit_price",
        "min_total_unit_price_so_far",
        "move_type_slug",
        F.lead("avg_unit_price", 2, 0).over(w_warehouse_id_move_type_by_mo).alias("next2_avg_unit_price"),
        F.nth_value("min_total_unit_price_so_far", 2).over(w_warehouse_id_move_type_by_mo_2).alias("second_min_total_unit_price_so_far"),
        F.min("avg_unit_price").over(w_all_by_avg_unit_price_runnin).alias("min_avg_unit_price_so_far"),
        F.round(F.percent_rank().over(w_warehouse_id_move_type_by_mo_3), 3).alias("move_type_warehouse_id_min_avg_unit_price_percen"),
        F.row_number().over(w_all_by_avg_unit_price).alias("rn"),
    )
)

result = (
    sequenced_2
    .filter(F.col("min_total_unit_price_so_far") <= 24000.2)
    .select(
        "move_type",
        "warehouse_id",
        "move_type_warehouse_id_min_avg_unit_price",
        "min_total_unit_price_so_far",
        "move_type_slug",
        "next2_avg_unit_price",
        "min_avg_unit_price_so_far",
        "rn",
        F.lower(F.regexp_replace(F.trim(F.col("move_type_slug")), " ", "_")).alias("move_type_slug_slug"),
    )
    .orderBy(
        F.col("min_total_unit_price_so_far").asc(),
        F.col("move_type_slug").asc(),
    )
)
