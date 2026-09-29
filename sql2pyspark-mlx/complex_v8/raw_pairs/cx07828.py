from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("move_type", "sku_id").orderBy(F.col("moved_at").desc(), F.col("id").desc())

recent_moves = (
    df_stock_moves
    .filter((F.col("operator") == "op_bee") | (F.col("unit_price") > 236.5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
    )
)

unique_rows = (
    recent_moves
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

annotated = (
    unique_rows
    .filter(F.col("move_type") == "TRANSFER")
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        F.concat_ws(": ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
    )
)

summary = (
    annotated
    .groupBy(
        "move_type",
        "warehouse_id",
        F.date_trunc("month", "moved_at_hour").alias("moved_at_hour_month"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("operator").alias("distinct_operators"),
    )
)

result = (
    summary
    .select(
        "move_type",
        "warehouse_id",
        "moved_at_hour_month",
        "n_rows",
        "distinct_operators",
        F.least(F.col("n_rows"), F.col("distinct_operators")).alias("least_n_rows_distinct_operators"),
        F.date_trunc("day", F.col("moved_at_hour_month")).alias("moved_at_hour_month_day"),
    )
    .orderBy(
        F.col("least_n_rows_distinct_operators").desc(),
        "n_rows",
        "move_type",
        "warehouse_id",
        "moved_at_hour_month",
    )
    .limit(20)
)
