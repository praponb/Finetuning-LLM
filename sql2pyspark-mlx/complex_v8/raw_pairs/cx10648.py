from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_commissioned_on_month_ye = Window.partitionBy("cnt", F.year("commissioned_on_month"))
w_all_by_cnt = Window.orderBy(F.col("cnt").desc(), "commissioned_on_month")
w_all_by_cnt_prior_3 = Window.orderBy(F.col("cnt").desc(), "commissioned_on_month").rowsBetween(-3, -1)
w_distinct_zones_last_commissi = Window.partitionBy("distinct_zones", F.month("last_commissioned_on")).orderBy("last_commissioned_on", "commissioned_on_month").rowsBetween(-1, -1)

scoped_sites = (
    df_sites
    .filter((F.col("installed_kw") >= 93.3) & (F.col("tariff") == "EV"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
    )
)

totals = (
    scoped_sites
    .groupBy(
        F.date_trunc("month", "commissioned_on").alias("commissioned_on_month"),
    )
    .agg(
        F.countDistinct("zone").alias("distinct_zones"),
        F.count("*").alias("cnt"),
        F.max("commissioned_on").alias("last_commissioned_on"),
    )
)

with_trends = (
    totals
    .select(
        "commissioned_on_month",
        "distinct_zones",
        "cnt",
        "last_commissioned_on",
        F.round(F.percent_rank().over(w_all_by_cnt), 3).alias("cnt_percent_rank"),
        F.max("distinct_zones").over(w_cnt_commissioned_on_month_ye).alias("cnt_commissioned_on_month_year_max_distinct_zone"),
        F.max("distinct_zones").over(w_all_by_cnt_prior_3).alias("distinct_zones_max_3_prior"),
        F.round(F.avg("cnt").over(w_distinct_zones_last_commissi), 2).alias("cnt_avg_1_prior"),
    )
)

derived = (
    with_trends
    .filter(F.col("last_commissioned_on") < F.lit("2025-05-10").cast("date"))
    .withColumn("commissioned_on_month_plus_15m", F.col("commissioned_on_month") + F.expr("INTERVAL 15 MINUTES"))
)

result = (
    derived
    .groupingSets(
        [["distinct_zones", "cnt"], ["distinct_zones"], []],
        "distinct_zones", "cnt",
    )
    .agg(
        F.max("commissioned_on_month").alias("last_commissioned_on_month"),
        F.round(F.sum("cnt_avg_1_prior"), 2).alias("total_cnt_avg_1_prior"),
    )
    .orderBy(F.col("distinct_zones").asc_nulls_last(), F.col("cnt").asc_nulls_last())
)
