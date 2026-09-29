from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_move_type_outbound_move_ty = Window.partitionBy("n_move_type_outbound", "move_type").orderBy(F.col("first_moved_at_hour").desc(), "moved_at_day", "moved_at_month_month", "move_type").rowsBetween(-1, Window.currentRow)
w_n_move_type_outbound_move_ty_2 = Window.partitionBy("n_move_type_outbound", "move_type").orderBy(F.col("first_moved_at_hour").desc(), "moved_at_day", "moved_at_month_month", "move_type").rowsBetween(-3, -1)
w_n_move_type_outbound_move_ty_3 = Window.partitionBy("n_move_type_outbound", "move_type").orderBy(F.col("first_moved_at_hour").desc(), "moved_at_day", "moved_at_month_month", "move_type").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_day_month_moved_at = Window.partitionBy(F.date_trunc("month", "moved_at_day"), F.year("moved_at_month_month"))

prepared_moves = (
    df_stock_moves
    .filter(F.col("move_type") == "OUTBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.date_trunc("month", F.col("moved_at")).alias("moved_at_month"),
    )
)

enriched = (
    prepared_moves
    .filter(F.col("qty") > 152)
    .select(
        "*",
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_ref"),
        F.quarter(F.col("moved_at")).alias("moved_at_quarter"),
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
    )
)

per_group = (
    enriched
    .groupBy(
        F.to_date("moved_at").alias("moved_at_day"),
        F.date_trunc("month", "moved_at_month").alias("moved_at_month_month"),
        "move_type",
    )
    .agg(
        F.sum(F.when(F.col("move_type") != "OUTBOUND", 1).otherwise(0)).alias("n_move_type_outbound"),
        F.count(F.when(F.col("move_type") == "TRANSFER", 1)).alias("n_move_type_transfer"),
        F.round(F.stddev("moved_at_quarter"), 2).alias("stddev_moved_at_quarter"),
        F.min("moved_at_hour").alias("first_moved_at_hour"),
    )
)

sequenced = (
    per_group
    .select(
        "*",
        F.sum("stddev_moved_at_quarter").over(w_n_move_type_outbound_move_ty).alias("stddev_moved_at_quarter_sum_2"),
        F.sum("n_move_type_transfer").over(w_n_move_type_outbound_move_ty_2).alias("n_move_type_transfer_sum_3_prior"),
        F.first("stddev_moved_at_quarter").over(w_n_move_type_outbound_move_ty_3).alias("first_stddev_moved_at_quarter"),
        F.max("n_move_type_outbound").over(w_moved_at_day_month_moved_at).alias("moved_at_day_month_moved_at_month_month_year_max"),
    )
)

result = (
    sequenced
    .select(
        "moved_at_day",
        "moved_at_month_month",
        F.col("stddev_moved_at_quarter_sum_2").alias("stddev_moved_at_quarter_sum_2_v"),
        F.col("stddev_moved_at_quarter").alias("stddev_moved_at_quarter_v"),
    )
    .unpivot(
        ids=["moved_at_day", "moved_at_month_month"],
        values=["stddev_moved_at_quarter_sum_2_v", "stddev_moved_at_quarter_v"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
