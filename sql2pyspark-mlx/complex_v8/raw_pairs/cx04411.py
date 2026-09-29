from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country = Window.partitionBy("country")
w_country_by_birth_year_full = Window.partitionBy("country").orderBy("birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_listeners
    .filter((F.col("signup_date") >= F.lit("2024-08-01").cast("date")) & (F.col("country") == "KR"))
    .select(
        "id",
        "country",
        "birth_year",
        "interests",
        F.concat_ws("-", F.col("country"), F.col("username")).alias("country_username_label"),
    )
)

with_trends = (
    cleaned
    .select(
        "id",
        "country",
        "birth_year",
        "interests",
        "country_username_label",
        F.round(F.avg("birth_year").over(w_country), 2).alias("country_avg_birth_year"),
        F.round(F.col("birth_year") - F.avg("birth_year").over(w_country), 2).alias("birth_year_vs_country_avg"),
        F.count(F.lit(1)).over(w_country).alias("rows_in_country"),
        F.nth_value("birth_year", 2).over(w_country_by_birth_year_full).alias("second_birth_year"),
    )
)

result = (
    with_trends
    .select("id", "rows_in_country", "birth_year")
    .unpivot(
        ids=["id"],
        values=["rows_in_country", "birth_year"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
