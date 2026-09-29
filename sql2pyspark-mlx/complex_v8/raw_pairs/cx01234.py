from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_opened_on_year = Window.partitionBy("city", F.year("opened_on"))

scoped_warehouses = (
    df_warehouses
    .filter(F.col("opened_on") < F.lit("2025-07-03").cast("date"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

with_trends = (
    scoped_warehouses
    .withColumn("rows_in_city_opened_on_year", F.count(F.lit(1)).over(w_city_opened_on_year))
    .withColumn("city_opened_on_year_min_capacity", F.min("capacity").over(w_city_opened_on_year))
)

screened = (
    with_trends
    .crossJoin(with_trends.agg(F.max("rows_in_city_opened_on_year").alias("overall_max_rows_in_city_opened_on_year")))
    .filter(F.col("rows_in_city_opened_on_year") < F.col("overall_max_rows_in_city_opened_on_year"))
    .drop("overall_max_rows_in_city_opened_on_year")
)

result = (
    screened
    .filter(F.col("rows_in_city_opened_on_year") >= 7)
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "rows_in_city_opened_on_year",
        "city_opened_on_year_min_capacity",
        F.concat_ws("|", F.col("city"), F.col("wh_code")).alias("city_wh_code_label"),
        F.dayofweek(F.col("opened_on")).alias("opened_on_dayofweek"),
    )
    .orderBy(F.col("rows_in_city_opened_on_year").asc())
)
