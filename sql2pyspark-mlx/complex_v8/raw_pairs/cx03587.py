from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_by_birth_year_runnin = Window.partitionBy("country").orderBy(F.col("birth_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_listeners = (
    df_listeners
    .filter((F.array_contains(F.col("interests"), "jazz")) & (F.col("plan").isNotNull()))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.date_add(F.col("signup_date"), 7).alias("signup_date_plus_7d"),
    )
)

windowed = (
    filtered_listeners
    .select(
        "id",
        "plan",
        "country",
        "interests",
        "signup_date_plus_7d",
        F.count(F.lit(1)).over(w_country_by_birth_year_runnin).alias("running_count"),
    )
)

result = (
    windowed
    .groupingSets(
        [["country", "plan"], ["country"], ["plan"]],
        "country", "plan",
    )
    .agg(
        F.grouping("country").alias("is_total_country"),
        F.round(F.avg("running_count"), 2).alias("avg_running_count"),
    )
    .select(
        F.coalesce(F.col("country"), F.lit("ALL")).alias("country"),
        F.coalesce(F.col("plan"), F.lit("ALL")).alias("plan"),
        "is_total_country",
        "avg_running_count",
    )
    .orderBy(F.col("country").asc_nulls_last(), F.col("plan").asc_nulls_last())
)
