from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter(F.col("end_date") >= F.lit("2025-10-12").cast("date"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.round(F.col("premium") * 0.9, 2).alias("premium_adj"),
        F.date_add(F.col("start_date"), 7).alias("start_date_plus_7d"),
    )
)

status_stats = (
    scoped_policys
    .groupBy("status")
    .agg(F.avg("premium").alias("avg_premium"))
)

above_average = (
    scoped_policys.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

result = (
    above_average
    .groupingSets(
        [["status", "holder_id"], ["status"], []],
        "status", "holder_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("premium_adj").alias("max_premium_adj"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "holder_id",
        "cnt",
        "distinct_ids",
        "max_premium_adj",
    )
)
