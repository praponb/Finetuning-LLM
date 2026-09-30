from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_by_capacity_full = Window.partitionBy("city").orderBy("capacity", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_city_by_capacity_ties = Window.partitionBy("city").orderBy("capacity")
w_all_by_capacity = Window.orderBy(F.col("capacity").desc(), "id")
w_all_by_capacity_full = Window.orderBy(F.col("capacity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("K%")) & (F.col("opened_on") >= F.lit("2024-04-13").cast("date")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.upper(F.trim(F.col("city"))).alias("city_clean"),
    )
)

metrics = (
    prepared
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        "city_clean",
        F.last("opened_on").over(w_city_by_capacity_full).alias("last_opened_on"),
        F.round(F.cume_dist().over(w_city_by_capacity_ties), 3).alias("capacity_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_capacity), 3).alias("capacity_cume_dist_2"),
        F.nth_value("opened_on", 2).over(w_all_by_capacity_full).alias("second_opened_on"),
    )
)

result = (
    metrics
    .select("id", "capacity_cume_dist", "capacity_cume_dist_2")
    .unpivot(
        ids=["id"],
        values=["capacity_cume_dist", "capacity_cume_dist_2"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
