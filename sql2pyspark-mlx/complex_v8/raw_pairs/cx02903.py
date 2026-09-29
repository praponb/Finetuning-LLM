from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month = Window.partitionBy(F.date_trunc("month", "end_date"))
w_all_by_premium_ties = Window.orderBy("premium")
w_all_by_premium_full = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium = Window.orderBy("premium", "id")

filtered = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2025-01-24").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.when(F.col("premium") >= 2945.9, "large").when(F.col("premium") >= 2319.8, "medium").otherwise("small").alias("premium_tier"),
    )
)

labeled = (
    filtered
    .select(
        "id",
        "product",
        "premium",
        "end_date",
        "premium_tier",
        F.date_trunc("month", F.col("end_date")).alias("end_date_month"),
    )
)

metrics = (
    labeled
    .select(
        "id",
        "product",
        "end_date",
        "end_date_month",
        F.round(F.col("premium") - F.avg("premium").over(w_end_date_month), 2).alias("premium_vs_end_date_month_avg"),
        F.size(F.collect_set("premium_tier").over(w_end_date_month)).alias("distinct_premium_tier_in_end_date_month"),
        F.round(F.percent_rank().over(w_all_by_premium_ties), 3).alias("premium_percent_rank"),
        F.first("premium").over(w_all_by_premium_full).alias("first_premium"),
        F.lead("end_date_month", 2).over(w_all_by_premium).alias("next2_end_date_month"),
    )
)

totals = (
    metrics
    .filter(F.col("premium_vs_end_date_month_avg") > -1000.0)
    .groupBy(
        F.date_trunc("month", "next2_end_date_month").alias("next2_end_date_month_month"),
        F.to_date("end_date_month").alias("end_date_month_day"),
    )
    .agg(
        F.countDistinct("product").alias("distinct_products"),
        F.round(F.avg("distinct_premium_tier_in_end_date_month"), 2).alias("avg_distinct_premium_tier_in_end_date_month"),
        F.min("end_date").alias("first_end_date"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.count("*").alias("n_rows"),
        F.avg("distinct_premium_tier_in_end_date_month").alias("avg_distinct_premium_tier_in_end_date_month_chec"),
    )
    .filter(F.col("avg_distinct_premium_tier_in_end_date_month_chec") > 4)
    .drop("avg_distinct_premium_tier_in_end_date_month_chec")
)

result = (
    totals
    .select(
        "next2_end_date_month_month",
        "end_date_month_day",
        "n_rows",
        "distinct_products",
    )
    .unpivot(
        ids=["next2_end_date_month_month", "end_date_month_day"],
        values=["n_rows", "distinct_products"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .orderBy("next2_end_date_month_month", "end_date_month_day", "field")
)
