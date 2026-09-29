from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_start_date_runn = Window.partitionBy("holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_start_date = Window.partitionBy("holder_id").orderBy("start_date", "id")
w_holder_id_by_start_date_ties = Window.partitionBy("holder_id").orderBy("start_date")
w_start_date_month_holder_id_b = Window.partitionBy(F.date_trunc("month", "start_date"), "holder_id").orderBy(F.col("start_date").desc(), "id")
w_start_date_quartile = Window.partitionBy("start_date_quartile")

cleaned = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-07-19").cast("date")) & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.first("start_date", ignorenulls=True).over(w_holder_id_by_start_date_runn).alias("first_known_start_date"),
        F.ntile(4).over(w_holder_id_by_start_date).alias("start_date_quartile"),
        F.round(F.percent_rank().over(w_holder_id_by_start_date_ties), 3).alias("start_date_percent_rank"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        F.lead("first_known_start_date", 3).over(w_start_date_month_holder_id_b).alias("next3_first_known_start_date"),
        F.max("start_date_quartile").over(w_start_date_quartile).alias("start_date_quartile_max_start_date_quartile"),
    )
)

result = (
    with_windows
    .filter(F.year(F.col("start_date")) == 2025)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        "first_known_start_date",
        "start_date_quartile",
        "start_date_percent_rank",
        "next3_first_known_start_date",
        "start_date_quartile_max_start_date_quartile",
    )
    .orderBy(F.col("status").asc(), "id")
    .limit(5)
)
