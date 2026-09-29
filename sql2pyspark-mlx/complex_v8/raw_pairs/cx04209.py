from pyspark.sql import functions as F

base = (
    df_policies
    .filter((F.col("holder_id") <= 3) & (F.col("status").isin("LAPSED", "EXPIRED", "ACTIVE")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.abs(F.col("premium") - 2096.6).alias("premium_distance"),
    )
)

totals = (
    base
    .filter(F.col("product_or_unknown") == "TRAVEL")
    .groupBy(F.date_trunc("month", "start_date").alias("start_date_month"))
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("product_or_unknown").alias("distinct_product_or_unknowns"),
    )
)

screened = (
    totals
    .crossJoin(totals.agg(F.max("cnt").alias("overall_max_cnt")))
    .filter(F.col("cnt") < F.col("overall_max_cnt"))
    .drop("overall_max_cnt")
)

screened_2 = (
    screened
    .crossJoin(screened.agg(F.avg("distinct_product_or_unknowns").alias("overall_avg_distinct_product_or_unknowns")))
    .filter(F.col("distinct_product_or_unknowns") > F.col("overall_avg_distinct_product_or_unknowns"))
    .drop("overall_avg_distinct_product_or_unknowns")
)

result = (
    screened_2
    .select("start_date_month", "distinct_product_or_unknowns")
    .orderBy(
        F.col("distinct_product_or_unknowns").desc(),
        F.col("start_date_month").asc(),
    )
)
