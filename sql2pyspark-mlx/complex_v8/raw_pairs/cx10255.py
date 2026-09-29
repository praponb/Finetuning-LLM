from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_opened_on_month_by_open = Window.partitionBy("city", F.month("opened_on")).orderBy("opened_on")
w_city_by_opened_on_running = Window.partitionBy("city").orderBy(F.col("opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_on_year_city = Window.partitionBy(F.year("opened_on"), "city")
w_city_opened_on_month_by_open_2 = Window.partitionBy("city", F.month("opened_on")).orderBy("opened_on", "id")

recent = (
    df_warehouses
    .filter(F.col("capacity").between(2614, 11506))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket"),
        F.concat(F.col("city"), F.lit("#"), F.col("id").cast("string")).alias("city_ref"),
    )
)

ranked = (
    recent
    .select(
        "*",
        F.dense_rank().over(w_city_opened_on_month_by_open).alias("opened_on_dense_rank"),
        F.max("capacity").over(w_city_by_opened_on_running).alias("max_capacity_so_far"),
        F.round(F.col("capacity") / F.nullif(F.sum("capacity").over(w_opened_on_year_city), F.lit(0)), 4).alias("capacity_share_of_opened_on_year_city"),
        F.lag("opened_on").over(w_city_opened_on_month_by_open_2).alias("prev_opened_on"),
    )
)

rollup_base = (
    ranked
    .groupBy(F.year("opened_on").alias("opened_on_year"), "city")
    .agg(
        F.min("capacity_share_of_opened_on_year_city").alias("min_capacity_share_of_opened_on_year_city"),
        F.max("capacity_share_of_opened_on_year_city").alias("max_capacity_share_of_opened_on_year_city"),
        F.max("capacity").alias("max_capacity"),
        F.round(F.avg("max_capacity_so_far"), 2).alias("avg_max_capacity_so_far"),
        F.sum("capacity_bucket").alias("sum_capacity_bucket_check"),
    )
    .filter(F.col("sum_capacity_bucket_check") > 5750)
    .drop("sum_capacity_bucket_check")
)

derived = (
    rollup_base
    .select(
        "*",
        F.concat(F.col("city"), F.lit("#"), F.col("opened_on_year").cast("string")).alias("city_ref"),
        (F.floor(F.col("max_capacity") / 5000) * 5000).alias("max_capacity_bucket"),
    )
)

qualified = (
    derived
    .crossJoin(derived.agg(F.avg("min_capacity_share_of_opened_on_year_city").alias("overall_avg_min_capacity_share_of_opened_on_year")))
    .filter(F.col("min_capacity_share_of_opened_on_year_city") > F.col("overall_avg_min_capacity_share_of_opened_on_year"))
    .drop("overall_avg_min_capacity_share_of_opened_on_year")
)

result = (
    qualified
    .select(
        "opened_on_year",
        "city",
        "min_capacity_share_of_opened_on_year_city",
        "max_capacity_share_of_opened_on_year_city",
        "avg_max_capacity_so_far",
        "city_ref",
        F.concat_ws(": ", F.col("city_ref"), F.col("city")).alias("city_ref_city_label"),
    )
    .orderBy(F.col("city_ref").desc(), F.col("city_ref_city_label").desc())
)
