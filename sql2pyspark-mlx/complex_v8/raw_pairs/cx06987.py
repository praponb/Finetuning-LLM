from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_on_plus_7d_tie = Window.orderBy("opened_on_plus_7d")
w_opened_on_year = Window.partitionBy("opened_on_year")
w_all_by_opened_on_full = Window.orderBy(F.col("opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_opened_on = Window.orderBy(F.col("opened_on").desc(), "id")

recent = (
    df_warehouses
    .filter((F.col("capacity") > 2614) & (F.upper(F.trim(F.col("wh_code"))).like("B%")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.concat_ws("-", F.col("city"), F.col("wh_code")).alias("city_wh_code_label"),
        F.date_add(F.col("opened_on"), 7).alias("opened_on_plus_7d"),
        F.year(F.col("opened_on")).alias("opened_on_year"),
    )
)

windowed = (
    recent
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        "city_wh_code_label",
        "opened_on_year",
        F.dense_rank().over(w_all_by_opened_on_plus_7d_tie).alias("opened_on_plus_7d_dense_rank"),
        F.round(F.col("opened_on_year") / F.nullif(F.sum("opened_on_year").over(w_opened_on_year), F.lit(0)), 4).alias("opened_on_year_share_of_opened_on_year"),
        F.count(F.lit(1)).over(w_opened_on_year).alias("rows_in_opened_on_year"),
    )
)

with_trends = (
    windowed
    .withColumn("first_opened_on", F.first("opened_on").over(w_all_by_opened_on_full))
    .withColumn("opened_on_rank", F.rank().over(w_all_by_opened_on))
)

first_rows = (
    with_trends
    .filter((F.col("opened_on_rank") <= 3) & (F.col("rows_in_opened_on_year") <= 17))
    .drop("opened_on_rank")
)

result = (
    first_rows
    .select(
        "id",
        "rows_in_opened_on_year",
        "opened_on_plus_7d_dense_rank",
        "capacity",
        "opened_on_year",
    )
    .unpivot(
        ids=["id"],
        values=["rows_in_opened_on_year", "opened_on_plus_7d_dense_rank", "capacity", "opened_on_year"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
