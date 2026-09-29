from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator")
w_sku_id_operator_by_qty = Window.partitionBy("sku_id", "operator").orderBy(F.col("qty").asc_nulls_last(), "id")
w_moved_at_plus_2h_month_by_un = Window.partitionBy(F.date_trunc("month", "moved_at_plus_2h")).orderBy(F.col("unit_price").desc(), "id")
w_moved_at_plus_2h_month_by_un_2 = Window.partitionBy(F.date_trunc("month", "moved_at_plus_2h")).orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_dedup = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), F.col("id").desc())

valid_moves = (
    df_stock_moves
    .filter(F.month(F.col("moved_at")).isin(1, 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 2 HOURS")).alias("moved_at_plus_2h"),
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
    )
)

ranked = (
    valid_moves
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "move_type_prefix",
        F.size(F.collect_set("sku_id").over(w_operator)).alias("distinct_sku_id_in_operator"),
        F.round(F.percent_rank().over(w_sku_id_operator_by_qty), 3).alias("qty_percent_rank"),
        F.row_number().over(w_moved_at_plus_2h_month_by_un).alias("unit_price_seq"),
        F.nth_value("qty", 2).over(w_moved_at_plus_2h_month_by_un_2).alias("second_qty"),
        F.round(F.col("qty") - F.avg("qty").over(w_operator), 2).alias("qty_vs_operator_avg"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
    )
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "move_type_prefix",
        "distinct_sku_id_in_operator",
        "qty_percent_rank",
        "unit_price_seq",
        "qty_vs_operator_avg",
        F.abs(F.col("unit_price_seq") - 7).alias("unit_price_seq_distance"),
    )
    .orderBy(F.col("unit_price_seq").desc(), "id")
    .limit(5)
)
