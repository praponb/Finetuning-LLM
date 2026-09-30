from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_end_date_year = Window.partitionBy("product", F.year("end_date"))
w_product_or_unknown_end_date = Window.partitionBy("product_or_unknown", F.date_trunc("month", "end_date"))
w_product_or_unknown_by_premiu = Window.partitionBy("product_or_unknown").orderBy("premium", "id").rowsBetween(-2, -1)
w_product_or_unknown_by_premiu_2 = Window.partitionBy("product_or_unknown").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_or_unknown_by_premiu_3 = Window.partitionBy("product_or_unknown").orderBy("premium", "id")
w_product_or_unknown = Window.partitionBy("product_or_unknown")
w_all_by_premium = Window.orderBy(F.col("premium").desc(), "id")
w_holder_id_by_premium_avg_2_p = Window.partitionBy("holder_id").orderBy("premium_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_premium_avg_2_p_2 = Window.partitionBy("holder_id").orderBy("premium_avg_2_prior", "id")

cleaned = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-03-27").cast("date"), F.lit("2025-03-23").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.round(F.col("premium") - F.avg("premium").over(w_product_end_date_year), 2).alias("premium_vs_product_end_date_year_avg"),
        F.max("premium").over(w_product_or_unknown_end_date).alias("product_or_unknown_end_date_month_max_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product_or_unknown_end_date), F.lit(0)), 4).alias("premium_share_of_product_or_unknown_end_date_mon"),
        F.round(F.avg("premium").over(w_product_or_unknown_by_premiu), 2).alias("premium_avg_2_prior"),
        F.round(F.avg("premium").over(w_product_end_date_year), 2).alias("product_end_date_year_avg_premium"),
        F.round(F.avg("premium").over(w_product_or_unknown_end_date), 2).alias("product_or_unknown_end_date_month_avg_premium"),
        F.nth_value("premium", 2).over(w_product_or_unknown_by_premiu_2).alias("second_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product_end_date_year), F.lit(0)), 4).alias("premium_share_of_product_end_date_year"),
        F.lag("premium", 2, 0).over(w_product_or_unknown_by_premiu_3).alias("prev2_premium"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "product_or_unknown",
        "premium_vs_product_end_date_year_avg",
        "product_or_unknown_end_date_month_max_premium",
        "premium_share_of_product_or_unknown_end_date_mon",
        "premium_avg_2_prior",
        "product_end_date_year_avg_premium",
        "second_premium",
        "premium_share_of_product_end_date_year",
        "prev2_premium",
        F.count(F.lit(1)).over(w_product_or_unknown).alias("rows_in_product_or_unknown"),
        F.rank().over(w_all_by_premium).alias("premium_rank"),
        F.max("prev2_premium").over(w_holder_id_by_premium_avg_2_p).alias("max_prev2_premium_so_far"),
        F.ntile(10).over(w_holder_id_by_premium_avg_2_p_2).alias("premium_avg_2_prior_decile"),
        F.round(F.avg("premium_avg_2_prior").over(w_product_or_unknown), 2).alias("product_or_unknown_avg_premium_avg_2_prior"),
    )
)

totals = (
    metrics
    .filter(F.col("rows_in_product_or_unknown") <= 15)
    .groupBy("holder_id")
    .agg(
        F.sum("max_prev2_premium_so_far").alias("total_max_prev2_premium_so_far"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("premium_share_of_product_end_date_year").alias("max_premium_share_of_product_end_date_year"),
        F.count("*").alias("cnt"),
    )
)

result = (
    totals
    .select(
        "holder_id",
        "total_max_prev2_premium_so_far",
        "distinct_ids",
        "max_premium_share_of_product_end_date_year",
        "cnt",
    )
    .orderBy(
        "max_premium_share_of_product_end_date_year",
        F.col("cnt").desc_nulls_first(),
    )
)
