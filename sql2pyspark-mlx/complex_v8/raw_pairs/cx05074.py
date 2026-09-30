from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("city").orderBy("opened_on", "id")
w_city = Window.partitionBy("city")
w_city_opened_on_month_by_capa = Window.partitionBy("city", F.month("opened_on")).orderBy("capacity", "id")

filtered = (
    df_warehouses
    .filter((F.col("capacity") <= 11506) & (F.col("opened_on").isNotNull()))
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        F.concat(F.col("city"), F.lit("#"), F.col("id").cast("string")).alias("city_ref"),
        F.concat(F.col("wh_code"), F.lit("#"), F.col("id").cast("string")).alias("wh_code_ref"),
        F.abs(F.col("capacity") - 4912).alias("capacity_distance"),
    )
)

unique_rows = (
    filtered
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

screened = (
    unique_rows.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.move_type") == "INBOUND"), "left_semi")
)

with_trends = (
    screened
    .select(
        "id",
        "opened_on",
        "wh_code_ref",
        F.sum("capacity_distance").over(w_city).alias("city_sum_capacity_distance"),
        F.min("capacity_distance").over(w_city).alias("city_min_capacity_distance"),
        F.round(F.col("capacity_distance") - F.avg("capacity_distance").over(w_city), 2).alias("capacity_distance_vs_city_avg"),
        F.ntile(4).over(w_city_opened_on_month_by_capa).alias("capacity_quartile"),
    )
)

result = (
    with_trends
    .select("id", "city_min_capacity_distance", "city_sum_capacity_distance")
    .unpivot(
        ids=["id"],
        values=["city_min_capacity_distance", "city_sum_capacity_distance"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
