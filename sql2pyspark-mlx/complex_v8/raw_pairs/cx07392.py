from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city = Window.partitionBy("city")
w_city_by_n_city_in_kuala_lump = Window.partitionBy("city").orderBy(F.col("n_city_in_kuala_lumpur_bangkok").desc(), "city", "opened_on_year")
w_city_by_n_city_in_kuala_lump_2 = Window.partitionBy("city").orderBy(F.col("n_city_in_kuala_lumpur_bangkok").desc(), "city", "opened_on_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_city_by_n_city_in_kuala_lump_3 = Window.partitionBy("city").orderBy(F.col("n_city_in_kuala_lumpur_bangkok").desc())
w_all_by_n_city_is_not_null = Window.orderBy(F.col("n_city_is_not_null").asc_nulls_last(), "city", "opened_on_year")

scoped = (
    df_warehouses
    .filter((F.lower(F.col("wh_code")).like("%kk%")) & (F.col("opened_on").isNotNull()))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
    )
)

with_activity = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.qty") > 124), "left_semi")
)

totals = (
    with_activity
    .filter(F.col("capacity_filled") < 13350)
    .groupBy("city", F.year("opened_on").alias("opened_on_year"))
    .agg(
        F.sum(F.when(F.col("city").isNotNull(), 1).otherwise(0)).alias("n_city_is_not_null"),
        F.count(F.when(F.col("city").isin("Kuala Lumpur", "Bangkok"), 1)).alias("n_city_in_kuala_lumpur_bangkok"),
        F.sum(F.when(F.col("city") == "Kuala Lumpur", 1).otherwise(0)).alias("n_city_kuala_lumpur"),
        F.sum(F.when(F.col("wh_code").isNotNull(), F.col("days_since_opened_on"))).alias("days_since_opened_on_wh_code_is_not_null"),
    )
)

without_matches = (
    totals
    .crossJoin(totals.agg(F.avg("n_city_kuala_lumpur").alias("overall_avg_n_city_kuala_lumpur")))
    .filter(F.col("n_city_kuala_lumpur") > F.col("overall_avg_n_city_kuala_lumpur"))
    .drop("overall_avg_n_city_kuala_lumpur")
)

windowed = (
    without_matches
    .select(
        "city",
        "opened_on_year",
        "n_city_is_not_null",
        "n_city_in_kuala_lumpur_bangkok",
        "n_city_kuala_lumpur",
        "days_since_opened_on_wh_code_is_not_null",
        F.round(F.avg("n_city_kuala_lumpur").over(w_city), 2).alias("city_avg_n_city_kuala_lumpur"),
        (F.col("n_city_kuala_lumpur") - F.lag("n_city_kuala_lumpur").over(w_city_by_n_city_in_kuala_lump)).alias("n_city_kuala_lumpur_change"),
        F.last("opened_on_year", ignorenulls=True).over(w_city_by_n_city_in_kuala_lump_2).alias("last_known_opened_on_year"),
        F.round(F.cume_dist().over(w_city_by_n_city_in_kuala_lump_3), 3).alias("n_city_in_kuala_lumpur_bangkok_cume_dist"),
        F.lag("n_city_kuala_lumpur").over(w_all_by_n_city_is_not_null).alias("prev_n_city_kuala_lumpur"),
    )
)

filtered = (
    windowed
    .crossJoin(windowed.agg(F.max("opened_on_year").alias("overall_max_opened_on_year")))
    .filter(F.col("opened_on_year") < F.col("overall_max_opened_on_year"))
    .drop("overall_max_opened_on_year")
)

result = (
    filtered
    .select(
        "city",
        "opened_on_year",
        F.col("prev_n_city_kuala_lumpur").alias("m_prev_n_city_kuala_lumpur"),
        F.col("n_city_kuala_lumpur").alias("n_city_kuala_lumpur_v"),
        F.col("last_known_opened_on_year").alias("last_known_opened_on_year_v"),
    )
    .unpivot(
        ids=["city", "opened_on_year"],
        values=["m_prev_n_city_kuala_lumpur", "n_city_kuala_lumpur_v", "last_known_opened_on_year_v"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .orderBy("city", "opened_on_year", "field")
)
