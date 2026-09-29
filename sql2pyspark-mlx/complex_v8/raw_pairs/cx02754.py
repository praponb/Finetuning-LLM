from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_unit_price_prior = Window.partitionBy("operator").orderBy("unit_price", "id").rowsBetween(-3, -1)
w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_operator = Window.partitionBy("operator")
w_all_by_unit_price_ties = Window.orderBy("unit_price")
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc_nulls_first(), "id")
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy("unit_price", "id")
w_unit_price_decile_operator_b = Window.partitionBy("unit_price_decile", "operator").orderBy(F.col("unit_price_cume_dist").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_unit_price_decile_operator_b_2 = Window.partitionBy("unit_price_decile", "operator").orderBy(F.col("unit_price_cume_dist").asc_nulls_last())
w_n_operator_in_op_eve_op_chan = Window.partitionBy("n_operator_in_op_eve_op_chan_o")
w_all_by_n_operator_not_in_op = Window.orderBy(F.col("n_operator_not_in_op_dew_op_ch").desc(), "operator", "warehouse_id", "distinct_sku_id_in_operator").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_n_operator_not_in_op_2 = Window.orderBy(F.col("n_operator_not_in_op_dew_op_ch").desc(), "operator", "warehouse_id", "distinct_sku_id_in_operator").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_stock_moves
    .filter((F.col("operator").isNotNull()) & (F.col("warehouse_id") <= 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
    )
)

ranked = (
    cleaned
    .select(
        "*",
        F.round(F.avg("qty").over(w_operator_by_unit_price_prior), 2).alias("qty_avg_3_prior"),
        F.max("qty").over(w_sku_id_warehouse_id).alias("sku_id_warehouse_id_max_qty"),
        F.round(F.col("qty") - F.avg("qty").over(w_operator), 2).alias("qty_vs_operator_avg"),
        F.round(F.cume_dist().over(w_all_by_unit_price_ties), 3).alias("unit_price_cume_dist"),
        F.ntile(10).over(w_all_by_unit_price).alias("unit_price_decile"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator), F.lit(0)), 4).alias("unit_price_share_of_operator"),
        (F.col("qty") - F.lag("qty").over(w_sku_id_by_unit_price)).alias("qty_change"),
        F.size(F.collect_set("sku_id").over(w_operator)).alias("distinct_sku_id_in_operator"),
    )
)

derived = (
    ranked
    .select(
        "*",
        F.initcap(F.trim(F.col("operator"))).alias("operator_clean"),
        F.concat(F.col("operator"), F.lit("#"), F.col("qty_change").cast("string")).alias("operator_ref"),
        F.abs(F.col("unit_price") - 255.6).alias("unit_price_distance"),
    )
)

sequenced = (
    derived
    .select(
        "*",
        F.round(F.col("qty_avg_3_prior") / F.nullif(F.sum("qty_avg_3_prior").over(w_operator), F.lit(0)), 4).alias("qty_avg_3_prior_share_of_operator"),
        F.max("unit_price_cume_dist").over(w_operator).alias("operator_max_unit_price_cume_dist"),
        F.round(F.col("unit_price_decile") / F.nullif(F.sum("unit_price_decile").over(w_operator), F.lit(0)), 4).alias("unit_price_decile_share_of_operator"),
        F.first("unit_price_cume_dist", ignorenulls=True).over(w_unit_price_decile_operator_b).alias("first_known_unit_price_cume_dist"),
        F.dense_rank().over(w_unit_price_decile_operator_b_2).alias("unit_price_cume_dist_dense_rank"),
        F.round(F.col("sku_id_warehouse_id_max_qty") - F.avg("sku_id_warehouse_id_max_qty").over(w_operator), 2).alias("sku_id_warehouse_id_max_qty_vs_operator_avg"),
        F.round(F.col("qty_change") - F.avg("qty_change").over(w_operator), 2).alias("qty_change_vs_operator_avg"),
    )
)

rollup_base = (
    sequenced
    .groupBy("operator", "warehouse_id", "distinct_sku_id_in_operator")
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.count(F.when(~F.col("operator").isin("op_dew", "op_chan"), 1)).alias("n_operator_not_in_op_dew_op_ch"),
        F.count(F.when(F.col("operator").isin("op_eve", "op_chan", "op_bee"), 1)).alias("n_operator_in_op_eve_op_chan_o"),
    )
)

sequenced_2 = (
    rollup_base
    .select(
        "*",
        F.round(F.col("distinct_sku_id_in_operator") - F.avg("distinct_sku_id_in_operator").over(w_n_operator_in_op_eve_op_chan), 2).alias("distinct_sku_id_in_operator_vs_n_operator_in_op_"),
        F.nth_value("distinct_sku_id_in_operator", 2).over(w_all_by_n_operator_not_in_op).alias("second_distinct_sku_id_in_operator"),
        F.round(F.avg("distinct_sku_id_in_operator").over(w_all_by_n_operator_not_in_op_2), 2).alias("distinct_sku_id_in_operator_avg_7"),
    )
)

result = (
    sequenced_2
    .select(
        "operator",
        "distinct_sku_id_in_operator",
        "operator_list",
        "n_operator_not_in_op_dew_op_ch",
        "n_operator_in_op_eve_op_chan_o",
        "distinct_sku_id_in_operator_vs_n_operator_in_op_",
        "distinct_sku_id_in_operator_avg_7",
    )
    .orderBy(
        F.col("n_operator_in_op_eve_op_chan_o").asc_nulls_last(),
        F.col("n_operator_not_in_op_dew_op_ch").desc_nulls_first(),
    )
)
