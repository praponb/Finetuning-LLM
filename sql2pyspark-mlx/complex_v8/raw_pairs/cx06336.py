from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_group_warehouse_cit = Window.partitionBy("operator_group", "warehouse_city")
w_warehouse_city_by_warehouse = Window.partitionBy("warehouse_city").orderBy("warehouse_capacity", "id")
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")

filtered = (
    df_stock_moves
    .filter(F.col("qty") >= 123)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.when(F.col("operator").isin("op_dew", "op_ann"), "group_a").when(F.col("operator") == "op_chan", "group_b").otherwise("other").alias("operator_group"),
    )
)

combined_details_warehouse = (
    filtered.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price",
        "s.operator_group",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

windowed = (
    combined_details_warehouse
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator_group",
        "warehouse_city",
        "warehouse_wh_code",
        "warehouse_capacity",
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator_group_warehouse_cit), F.lit(0)), 4).alias("unit_price_share_of_operator_group_warehouse_cit"),
        (F.col("unit_price") - F.lag("unit_price").over(w_warehouse_city_by_warehouse)).alias("unit_price_change"),
        F.round(F.avg("unit_price").over(w_operator_group_warehouse_cit), 2).alias("operator_group_warehouse_city_avg_unit_price"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_operator_group_warehouse_cit), 2).alias("warehouse_capacity_vs_operator_group_warehouse_c"),
        F.lead("unit_price", 3).over(w_all_by_moved_at).alias("next3_unit_price"),
        F.ntile(4).over(w_warehouse_city_by_warehouse).alias("warehouse_capacity_quartile"),
        F.round(F.avg("unit_price").over(w_warehouse_id_move_type), 2).alias("warehouse_id_move_type_avg_unit_price"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_warehouse_id_move_type), 2).alias("warehouse_capacity_vs_warehouse_id_move_type_avg"),
    )
)

derived = (
    windowed
    .withColumn("least_next3_unit_price_unit_price_change", F.least(F.col("next3_unit_price"), F.col("unit_price_change")))
)

result = (
    derived
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "warehouse_city",
        "warehouse_wh_code",
        "warehouse_capacity",
        "unit_price_change",
        "operator_group_warehouse_city_avg_unit_price",
        "warehouse_capacity_vs_operator_group_warehouse_c",
        "warehouse_capacity_quartile",
        "warehouse_capacity_vs_warehouse_id_move_type_avg",
        "least_next3_unit_price_unit_price_change",
        F.concat(F.col("warehouse_city"), F.lit("#"), F.col("warehouse_capacity").cast("string")).alias("warehouse_city_ref"),
    )
)
