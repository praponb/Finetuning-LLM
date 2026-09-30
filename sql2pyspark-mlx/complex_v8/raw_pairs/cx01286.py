from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("city").orderBy(F.col("capacity_bucket").desc(), F.col("id").desc(), F.col("move_id").desc())

cleaned = (
    df_warehouses
    .filter(F.col("opened_on") < F.lit("2025-02-14").cast("date"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket"),
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket_2"),
        F.date_trunc("month", F.col("opened_on")).alias("opened_on_month"),
    )
)

with_children = (
    cleaned.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.wh_code",
        "s.city",
        "s.capacity",
        "s.capacity_bucket",
        "s.capacity_bucket_2",
        "s.opened_on_month",
        F.col("st.id").alias("move_id"),
        "st.move_type",
        F.col("st.qty").alias("move_qty"),
    )
)

qualified = (
    with_children.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.operator").isNotNull()), "left_anti")
)

latest_per_key = (
    qualified
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "wh_code",
        "capacity_bucket_2",
        "opened_on_month",
        "move_id",
        "move_type",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_on_month")).alias("days_since_opened_on_month"),
        F.coalesce(F.col("move_qty"), F.lit(0)).alias("move_qty_filled"),
    )
    .orderBy(F.col("capacity_bucket_2").asc_nulls_last())
)
