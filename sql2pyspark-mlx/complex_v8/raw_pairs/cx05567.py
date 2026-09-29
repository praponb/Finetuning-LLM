from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_pct_move_type_is_not = Window.orderBy("pct_move_type_is_not_null", "moved_at_hour")
w_moved_at_hour_cnt = Window.partitionBy("moved_at_hour", "cnt")
w_moved_at_hour_cnt_by_cnt = Window.partitionBy("moved_at_hour", "cnt").orderBy("cnt", "moved_at_hour")

scoped_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.hour(F.col("moved_at")).alias("moved_at_hour"),
    )
)

summary = (
    scoped_moves
    .filter(F.col("sku_id") <= 6)
    .groupBy("moved_at_hour")
    .agg(
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.count("*").alias("cnt"),
        F.max("moved_at").alias("last_moved_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(100 * F.count(F.when(F.col("move_type").isNotNull(), 1)) / F.count("*"), 1).alias("pct_move_type_is_not_null"),
    )
)

with_activity = (
    summary
    .crossJoin(summary.agg(F.avg("moved_at_hour").alias("overall_avg_moved_at_hour")))
    .filter(F.col("moved_at_hour") > F.col("overall_avg_moved_at_hour"))
    .drop("overall_avg_moved_at_hour")
)

windowed = (
    with_activity
    .select(
        "*",
        F.lead("moved_at_hour", 2).over(w_all_by_pct_move_type_is_not).alias("next2_moved_at_hour"),
        F.min("distinct_ids").over(w_moved_at_hour_cnt).alias("moved_at_hour_cnt_min_distinct_ids"),
        F.row_number().over(w_moved_at_hour_cnt_by_cnt).alias("cnt_seq"),
    )
)

tagged = (
    windowed
    .select(
        "moved_at_hour",
        "move_type_list",
        "cnt",
        "last_moved_at",
        "distinct_ids",
        "pct_move_type_is_not_null",
        "next2_moved_at_hour",
        "moved_at_hour_cnt_min_distinct_ids",
        "cnt_seq",
        F.month(F.col("last_moved_at")).alias("last_moved_at_month"),
    )
)

result = (
    tagged
    .select(
        "moved_at_hour",
        "last_moved_at_month",
        "distinct_ids",
        "cnt",
        "cnt_seq",
    )
    .unpivot(
        ids=["moved_at_hour"],
        values=["last_moved_at_month", "distinct_ids", "cnt", "cnt_seq"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
