from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator_or_unknown").orderBy(F.col("qty").desc(), F.col("id").desc())

prepared_moves = (
    df_stock_moves
    .filter((F.col("qty") > 169) & (~F.col("move_type").isin("ADJUST", "OUTBOUND")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

enriched = (
    prepared_moves
    .select(
        "*",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
        F.concat_ws(": ", F.col("operator"), F.col("operator_or_unknown")).alias("operator_operator_or_unknown_label"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_activity = (
    latest.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id").isNotNull()) & (F.col("st.unit_price") > 206.8), "left_semi")
)

grouped = (
    with_activity
    .groupBy(
        F.date_trunc("month", "moved_at").alias("moved_at_month"),
        "sku_id",
        "warehouse_id",
    )
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.round(F.sum("qty") / F.nullif(F.sum("moved_at_dayofweek"), F.lit(0)), 3).alias("qty_to_moved_at_dayofweek_ratio"),
        F.sum(F.when(F.col("operator") == "op_ann", F.col("moved_at_dayofweek")).otherwise(0)).alias("moved_at_dayofweek_operator_op_ann"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("qty").alias("total_qty"),
        F.max("moved_at_dayofweek").alias("max_moved_at_dayofweek"),
        F.max("qty").alias("max_qty_check"),
    )
    .filter(F.col("max_qty_check") > 169)
    .drop("max_qty_check")
)

result = (
    grouped
    .rollup("warehouse_id", "max_moved_at_dayofweek", "sku_id")
    .agg(
        F.grouping("warehouse_id").alias("is_total_warehouse_id"),
        F.percentile_approx("max_moved_at_dayofweek", 0.5).alias("median_max_moved_at_dayofweek"),
        F.min("moved_at_month").alias("first_moved_at_month"),
        F.round(F.stddev("max_moved_at_dayofweek"), 2).alias("stddev_max_moved_at_dayofweek"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("max_moved_at_dayofweek").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
