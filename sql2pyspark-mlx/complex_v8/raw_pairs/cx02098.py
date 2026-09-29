from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_by_capacity = Window.partitionBy("city").orderBy("capacity", "id")
w_opened_on_year_city = Window.partitionBy(F.year("opened_on"), "city")
w_city_by_capacity_moving_2 = Window.partitionBy("city").orderBy("capacity", "id").rowsBetween(-2, Window.currentRow)
w_all_by_capacity_share_of_ope = Window.orderBy(F.col("capacity_share_of_opened_on_year_city").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_capacity_share_of_ope_2 = Window.orderBy(F.col("capacity_share_of_opened_on_year_city").desc())

cleaned = df_warehouses.filter(F.col("city").isin("Kuala Lumpur", "Bangkok"))

without_matches = (
    cleaned.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.sku_id").isNotNull()), "left_semi")
    .filter(F.col("city") == "Kuala Lumpur")
)

metrics = (
    without_matches
    .select(
        "*",
        F.row_number().over(w_city_by_capacity).alias("capacity_seq"),
        F.round(F.col("capacity") / F.nullif(F.sum("capacity").over(w_opened_on_year_city), F.lit(0)), 4).alias("capacity_share_of_opened_on_year_city"),
        F.sum("capacity").over(w_opened_on_year_city).alias("opened_on_year_city_sum_capacity"),
        F.min("capacity").over(w_opened_on_year_city).alias("opened_on_year_city_min_capacity"),
        F.round(F.avg("capacity").over(w_city_by_capacity_moving_2), 2).alias("capacity_avg_3"),
    )
)

sequenced = (
    metrics
    .withColumn("capacity_avg_2", F.round(F.avg("capacity").over(w_all_by_capacity_share_of_ope), 2))
    .withColumn("capacity_share_of_opened_on_year_city_dense_rank", F.dense_rank().over(w_all_by_capacity_share_of_ope_2))
)

result = (
    sequenced
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        "capacity_seq",
        "capacity_share_of_opened_on_year_city",
        "opened_on_year_city_sum_capacity",
        "opened_on_year_city_min_capacity",
        "capacity_avg_2",
        F.concat(F.col("city"), F.lit("#"), F.col("capacity_seq").cast("string")).alias("city_ref"),
    )
)
