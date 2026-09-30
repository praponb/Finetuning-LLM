from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter(F.col("premium") > 1132.1)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_format(F.col("start_date"), "yyyy-MM").alias("start_date_label"),
    )
)

annotated = (
    cleaned
    .withColumn("start_date_label_ref", F.concat(F.col("start_date_label"), F.lit("#"), F.col("holder_id").cast("string")))
)

stats = (
    annotated
    .groupBy(F.date_trunc("month", "start_date").alias("start_date_month"))
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.countDistinct("status").alias("distinct_status"),
        F.round(F.avg("premium"), 2).alias("avg_premium"),
        F.round(100 * F.count(F.when(F.col("status") == "LAPSED", 1)) / F.count("*"), 1).alias("pct_status_lapsed"),
        F.avg("premium").alias("avg_premium_check"),
    )
    .filter(F.col("avg_premium_check") > 2033.8)
    .drop("avg_premium_check")
)

result = (
    stats
    .filter(F.col("avg_premium").between(440.0, 3080.0))
    .select(
        "start_date_month",
        "cnt",
        "pct_status_lapsed",
        (F.col("cnt") - F.col("pct_status_lapsed")).alias("cnt_minus_pct_status_lapsed"),
        F.abs(F.col("avg_premium") - 3080.0).alias("avg_premium_distance"),
    )
    .orderBy(
        F.col("cnt").asc_nulls_last(),
        "cnt_minus_pct_status_lapsed",
        "start_date_month",
    )
    .limit(20)
)
