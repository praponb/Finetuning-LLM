from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_capacity_tier_city = Window.partitionBy("capacity_tier", "city")
w_capacity_tier_by_capacity_ci = Window.partitionBy("capacity_tier").orderBy(F.col("capacity_city_not_in_kuala_lumpur_ban").desc(), "city", "capacity_tier").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_capacity_tier_by_capacity_ci_2 = Window.partitionBy("capacity_tier").orderBy(F.col("capacity_city_not_in_kuala_lumpur_ban").desc(), "city", "capacity_tier")

cleaned_warehouses = (
    df_warehouses
    .filter((F.col("capacity").between(2614, 11506)) & (F.year(F.col("opened_on")) == 2024))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

grouped = (
    cleaned_warehouses
    .groupBy(
        "city",
        F.when(F.col("capacity") >= 11506, "HIGH").when(F.col("capacity") >= 4912, "MEDIUM").otherwise("LOW").alias("capacity_tier"),
    )
    .agg(
        F.sum(F.when(~F.col("city").isin("Kuala Lumpur", "Bangkok"), F.col("capacity"))).alias("capacity_city_not_in_kuala_lumpur_ban"),
        F.round(F.stddev("capacity"), 2).alias("stddev_capacity"),
        F.max("capacity").alias("max_capacity_check"),
    )
    .filter(F.col("max_capacity_check") > 2614)
    .drop("max_capacity_check")
)

sequenced = (
    grouped
    .select(
        "*",
        F.max("stddev_capacity").over(w_capacity_tier_city).alias("capacity_tier_city_max_stddev_capacity"),
        F.first("stddev_capacity").over(w_capacity_tier_by_capacity_ci).alias("first_stddev_capacity"),
        F.round(F.avg("stddev_capacity").over(w_capacity_tier_city), 2).alias("capacity_tier_city_avg_stddev_capacity"),
        F.ntile(3).over(w_capacity_tier_by_capacity_ci_2).alias("capacity_city_not_in_kuala_lumpur_ban_tercile"),
        F.round(F.col("stddev_capacity") - F.avg("stddev_capacity").over(w_capacity_tier_city), 2).alias("stddev_capacity_vs_capacity_tier_city_avg"),
    )
)

result = (
    sequenced
    .select(
        "city",
        "capacity_tier",
        "capacity_tier_city_max_stddev_capacity",
        "capacity_tier_city_avg_stddev_capacity",
    )
    .unpivot(
        ids=["city", "capacity_tier"],
        values=["capacity_tier_city_max_stddev_capacity", "capacity_tier_city_avg_stddev_capacity"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .orderBy("city", "capacity_tier", "metric_name")
)
