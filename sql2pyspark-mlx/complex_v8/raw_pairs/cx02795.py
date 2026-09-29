from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("city").orderBy(F.col("opened_on").desc(), F.col("id").desc())
w_all_by_opened_on = Window.orderBy(F.col("opened_on").asc_nulls_last(), "id")
w_city = Window.partitionBy("city")
w_latest = Window.partitionBy("city").orderBy("prev_capacity", "id")

recent = (
    df_warehouses
    .filter((F.col("opened_on") < F.lit("2025-02-14").cast("date")) & (F.lower(F.col("wh_code")).like("%kk%")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.lower(F.regexp_replace(F.trim(F.col("city")), " ", "_")).alias("city_slug"),
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
    )
)

deduped = (
    recent
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

ranked = (
    deduped
    .withColumn("prev_capacity", F.lag("capacity").over(w_all_by_opened_on))
    .withColumn("capacity_filled_vs_city_avg", F.round(F.col("capacity_filled") - F.avg("capacity_filled").over(w_city), 2))
)

deduped_2 = (
    ranked
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

city_stats = (
    deduped_2
    .groupBy("city")
    .agg(F.max("capacity").alias("max_capacity"))
)

vs_group = (
    deduped_2.alias("s")
    .join(city_stats.alias("g"), F.col("g.city") == F.col("s.city"), "inner")
    .filter(F.col("s.capacity") == F.col("g.max_capacity"))
    .select(
        "s.*",
        "g.max_capacity",
        F.round(F.col("s.capacity") - F.col("g.max_capacity"), 2).alias("capacity_minus_max"),
    )
)

result = (
    vs_group
    .select(
        "id",
        F.col("capacity_filled_vs_city_avg").alias("m_capacity_filled_vs_city_avg"),
        F.col("capacity_minus_max").alias("m_capacity_minus_max"),
    )
    .unpivot(
        ids=["id"],
        values=["m_capacity_filled_vs_city_avg", "m_capacity_minus_max"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
