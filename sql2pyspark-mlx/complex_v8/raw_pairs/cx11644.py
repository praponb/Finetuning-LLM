from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_avg_least_qty_unit_pr = Window.orderBy("avg_least_qty_unit_price", "move_type", "moved_at_year")
w_cnt_first_moved_at_month = Window.partitionBy("cnt", F.date_trunc("month", "first_moved_at"))

recent = (
    df_stock_moves
    .filter(F.col("operator") != "op_ann")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
    )
)

summary = (
    recent
    .groupBy("move_type", F.year("moved_at").alias("moved_at_year"))
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.round(F.stddev("least_qty_unit_price"), 2).alias("stddev_least_qty_unit_price"),
        F.round(F.avg("least_qty_unit_price"), 2).alias("avg_least_qty_unit_price"),
        F.count("*").alias("cnt"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

ranked = (
    summary
    .select(
        "*",
        F.row_number().over(w_all_by_avg_least_qty_unit_pr).alias("seq_no"),
        F.count(F.lit(1)).over(w_cnt_first_moved_at_month).alias("rows_in_cnt_first_moved_at_month"),
        F.round(F.col("moved_at_year") / F.nullif(F.sum("moved_at_year").over(w_cnt_first_moved_at_month), F.lit(0)), 4).alias("moved_at_year_share_of_cnt_first_moved_at_month"),
    )
)

scored = (
    ranked
    .filter(F.col("avg_least_qty_unit_price").between(100.8, 325.4))
    .select(
        "*",
        F.greatest(F.col("cnt"), F.col("moved_at_year")).alias("greatest_cnt_moved_at_year"),
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
    )
)

result = (
    scored
    .select(
        "move_type",
        "moved_at_year",
        "rows_in_cnt_first_moved_at_month",
        "cnt",
    )
    .unpivot(
        ids=["move_type", "moved_at_year"],
        values=["rows_in_cnt_first_moved_at_month", "cnt"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("move_type", "moved_at_year", "metric_name")
)
