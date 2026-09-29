from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_on = Window.orderBy(F.col("opened_on").desc(), "id")
w_opened_on_year = Window.partitionBy(F.year("opened_on"))
w_all_by_opened_on_full = Window.orderBy("opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_warehouses
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("city")), " ", "_")).alias("city_slug"),
    )
)

qualified = (
    scoped
    .crossJoin(scoped.agg(F.min("capacity_bucket").alias("overall_min_capacity_bucket")))
    .filter(F.col("capacity_bucket") > F.col("overall_min_capacity_bucket"))
    .drop("overall_min_capacity_bucket")
)

sequenced = (
    qualified
    .select(
        "*",
        (F.col("capacity") - F.lag("capacity").over(w_all_by_opened_on)).alias("capacity_change"),
        F.round(F.col("capacity_bucket") / F.nullif(F.sum("capacity_bucket").over(w_opened_on_year), F.lit(0)), 4).alias("capacity_bucket_share_of_opened_on_year"),
        F.sum("capacity").over(w_opened_on_year).alias("opened_on_year_sum_capacity"),
        F.round(F.col("capacity") - F.avg("capacity").over(w_opened_on_year), 2).alias("capacity_vs_opened_on_year_avg"),
        F.nth_value("capacity", 2).over(w_all_by_opened_on_full).alias("second_capacity"),
    )
)

result = (
    sequenced
    .filter(F.col("opened_on_year_sum_capacity") > 60700)
    .select(
        "id",
        "capacity",
        "capacity_bucket",
        "city_slug",
        "capacity_change",
        "capacity_bucket_share_of_opened_on_year",
        "capacity_vs_opened_on_year_avg",
        "second_capacity",
        F.trunc(F.col("opened_on"), "MM").alias("opened_on_month_start"),
    )
    .orderBy(
        F.col("second_capacity").desc_nulls_first(),
        F.col("id").asc_nulls_last(),
    )
    .limit(20)
)
