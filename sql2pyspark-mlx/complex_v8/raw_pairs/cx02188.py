from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_by_opened_on_running = Window.partitionBy("city").orderBy("opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_city_by_opened_on_full = Window.partitionBy("city").orderBy("opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_on_month_city = Window.partitionBy(F.month("opened_on"), "city")
w_all_by_capacity = Window.orderBy(F.col("capacity").asc_nulls_last(), "id")
w_all_by_opened_on_full = Window.orderBy(F.col("opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_warehouses = (
    df_warehouses
    .filter((F.col("city") == "Singapore") & (F.lower(F.col("wh_code")).like("%x1%")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

sequenced = (
    valid_warehouses
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.first("opened_on", ignorenulls=True).over(w_city_by_opened_on_running).alias("first_known_opened_on"),
        F.nth_value("opened_on", 2).over(w_city_by_opened_on_full).alias("second_opened_on"),
        F.lag("opened_on").over(w_all_by_capacity).alias("prev_opened_on"),
        (F.col("capacity") - F.lag("capacity").over(w_all_by_capacity)).alias("capacity_change"),
        F.nth_value("opened_on", 2).over(w_all_by_opened_on_full).alias("second_opened_on_2"),
    )
)

city_stats = (
    sequenced
    .groupBy("city")
    .agg(F.avg("capacity").alias("avg_capacity"))
)

vs_group = (
    sequenced.alias("s")
    .join(city_stats.alias("g"), F.col("g.city") == F.col("s.city"), "inner")
    .filter(F.col("s.capacity") > F.col("g.avg_capacity"))
    .select(
        "s.*",
        "g.avg_capacity",
        F.round(F.col("s.capacity") - F.col("g.avg_capacity"), 2).alias("capacity_minus_avg"),
    )
)

result = (
    vs_group
    .select("id", "capacity_minus_avg", "avg_capacity")
    .unpivot(
        ids=["id"],
        values=["capacity_minus_avg", "avg_capacity"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
)
