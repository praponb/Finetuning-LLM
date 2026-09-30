from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city = Window.partitionBy("city")
w_city_by_capacity_ties = Window.partitionBy("city").orderBy("capacity")
w_all_by_capacity_running = Window.orderBy(F.col("capacity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_warehouses = (
    df_warehouses
    .filter(F.col("city") == "Singapore")
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
    )
)

with_activity = (
    filtered_warehouses.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.operator").isin("op_bee", "op_eve")) & (F.col("st.sku_id") <= 15), "left_semi")
)

ranked = (
    with_activity
    .select(
        "*",
        F.round(F.col("capacity") - F.avg("capacity").over(w_city), 2).alias("capacity_vs_city_avg"),
        F.rank().over(w_city_by_capacity_ties).alias("capacity_rank"),
        F.max("capacity").over(w_city).alias("city_max_capacity"),
        F.first("capacity", ignorenulls=True).over(w_all_by_capacity_running).alias("first_known_capacity"),
    )
)

tagged = (
    ranked
    .filter(F.col("capacity") < 11768)
    .select(
        "*",
        F.concat(F.col("wh_code"), F.lit("#"), F.col("city_max_capacity").cast("string")).alias("wh_code_ref"),
        F.least(F.col("capacity_vs_city_avg"), F.col("first_known_capacity")).alias("least_capacity_vs_city_avg_first_known_capacity"),
    )
)

result = (
    tagged
    .select(
        "id",
        "city_max_capacity",
        "capacity_rank",
        "capacity",
    )
    .unpivot(
        ids=["id"],
        values=["city_max_capacity", "capacity_rank", "capacity"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
