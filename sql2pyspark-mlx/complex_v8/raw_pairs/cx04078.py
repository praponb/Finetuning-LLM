from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_capacity_distance = Window.orderBy("capacity_distance", "id")
w_all_by_opened_on = Window.orderBy("opened_on", "id")
w_all_by_capacity_distance_run = Window.orderBy("capacity_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_on_prior_3 = Window.orderBy("opened_on", "id").rowsBetween(-3, -1)
w_all_by_capacity_full = Window.orderBy("capacity", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_warehouses = (
    df_warehouses
    .filter(F.col("opened_on") >= F.lit("2024-11-25").cast("date"))
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.abs(F.col("capacity") - 11506).alias("capacity_distance"),
    )
)

qualified = (
    filtered_warehouses.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.sku_id").isNotNull()) & (F.col("st.moved_at") < F.lit("2025-02-24 11:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("capacity_distance") >= 6000)
)

metrics = (
    qualified
    .select(
        "*",
        F.rank().over(w_all_by_capacity_distance).alias("capacity_distance_rank"),
        F.lag("capacity_distance", 2, 0).over(w_all_by_opened_on).alias("prev2_capacity_distance"),
        F.round(F.cume_dist().over(w_all_by_opened_on), 3).alias("opened_on_cume_dist"),
        F.first("opened_on", ignorenulls=True).over(w_all_by_capacity_distance_run).alias("first_known_opened_on"),
        F.max("capacity").over(w_all_by_opened_on_prior_3).alias("capacity_max_3_prior"),
        F.nth_value("capacity", 2).over(w_all_by_capacity_full).alias("second_capacity"),
    )
)

stats = (
    metrics
    .groupBy(
        F.date_trunc("month", "first_known_opened_on").alias("first_known_opened_on_month"),
        "capacity_distance_rank",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.min("capacity_distance_rank").alias("min_capacity_distance_rank"),
        F.sum("prev2_capacity_distance").alias("sum_prev2_capacity_distance_check"),
    )
    .filter(F.col("sum_prev2_capacity_distance_check") > 5000)
    .drop("sum_prev2_capacity_distance_check")
)

result = (
    stats
    .select(
        "first_known_opened_on_month",
        "n_rows",
        F.when(F.col("n_rows") >= 8, "HIGH").when(F.col("n_rows") >= 5, "MEDIUM").otherwise("LOW").alias("n_rows_tier"),
    )
    .orderBy("n_rows")
)
