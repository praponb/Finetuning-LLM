from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_year_status_by_prem = Window.partitionBy("end_date_year", "status").orderBy("premium_adj_to_status_len_ratio", "status", "product", "end_date_year")
w_product_distinct_ids = Window.partitionBy("product", "distinct_ids")

base_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.round(F.col("premium") * 1.07, 2).alias("premium_adj"),
    )
)

per_group = (
    base_policys
    .groupBy("status", "product", F.year("end_date").alias("end_date_year"))
    .agg(
        F.round(F.sum("status_len") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("status_len_to_premium_ratio"),
        F.sum("premium").alias("total_premium"),
        F.percentile_approx("premium", 0.5).alias("median_premium"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("premium_adj") / F.nullif(F.sum("status_len"), F.lit(0)), 3).alias("premium_adj_to_status_len_ratio"),
    )
)

scored = (
    per_group
    .filter(F.col("total_premium") < 16590.0)
    .select(
        "*",
        F.round(F.col("total_premium") / F.nullif(F.col("distinct_ids"), F.lit(0)), 3).alias("total_premium_per_distinct_ids"),
    )
)

ranked = (
    scored
    .select(
        "*",
        F.round(F.percent_rank().over(w_end_date_year_status_by_prem), 3).alias("premium_adj_to_status_len_ratio_percent_rank"),
        F.max("median_premium").over(w_product_distinct_ids).alias("product_distinct_ids_max_median_premium"),
        F.sum("median_premium").over(w_product_distinct_ids).alias("product_distinct_ids_sum_median_premium"),
        (F.col("end_date_year") - F.lag("end_date_year").over(w_end_date_year_status_by_prem)).alias("end_date_year_change"),
        F.ntile(4).over(w_end_date_year_status_by_prem).alias("premium_adj_to_status_len_ratio_quartile"),
        F.lead("end_date_year").over(w_end_date_year_status_by_prem).alias("next_end_date_year"),
    )
)

result = (
    ranked
    .select(
        "status",
        "product",
        "end_date_year",
        "status_len_to_premium_ratio",
        "total_premium_per_distinct_ids",
        "premium_adj_to_status_len_ratio_percent_rank",
        "product_distinct_ids_max_median_premium",
        "product_distinct_ids_sum_median_premium",
        "premium_adj_to_status_len_ratio_quartile",
    )
    .orderBy("end_date_year")
)
