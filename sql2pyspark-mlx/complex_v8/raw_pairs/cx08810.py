from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff = Window.partitionBy("tariff")
w_tariff_group_by_rate_full = Window.partitionBy("tariff_group").orderBy(F.col("rate").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_group_tariff_by_rate = Window.partitionBy("tariff_group", "tariff").orderBy(F.col("rate").desc_nulls_first(), "id")

scoped_tariffs = (
    df_tariff_windows
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
        F.when(F.col("tariff").isin("EV", "TOU"), "group_a").when(F.col("tariff") == "FLAT", "group_b").otherwise("other").alias("tariff_group"),
    )
)

ranked = (
    scoped_tariffs
    .select(
        "id",
        "tariff",
        "tariff_slug",
        F.size(F.collect_set("tariff_group").over(w_tariff)).alias("distinct_tariff_group_in_tariff"),
        F.first("rate").over(w_tariff_group_by_rate_full).alias("first_rate"),
        F.row_number().over(w_tariff_group_tariff_by_rate).alias("rn"),
        F.count(F.lit(1)).over(w_tariff).alias("rows_in_tariff"),
    )
)

rollup_base = (
    ranked
    .groupBy("tariff", "rn")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("rows_in_tariff"), 2).alias("avg_rows_in_tariff"),
        F.countDistinct("tariff").alias("distinct_tariffs"),
        F.array_sort(F.collect_set("tariff")).alias("tariff_list"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    rollup_base
    .select(
        "tariff",
        "rn",
        F.col("cnt").alias("cnt_v"),
        F.col("distinct_tariffs").alias("distinct_tariffs_v"),
    )
    .unpivot(
        ids=["tariff", "rn"],
        values=["cnt_v", "distinct_tariffs_v"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("tariff", "rn", "metric")
)
