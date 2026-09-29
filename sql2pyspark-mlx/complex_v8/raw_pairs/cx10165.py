from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product = Window.partitionBy("holder_id", "product")
w_all_by_end_date_full = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date_ties = Window.orderBy(F.col("end_date").desc())
w_all_by_end_date = Window.orderBy("end_date", "id")

prepared_policys = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-12-02").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

joined_holder = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.region").isNotNull())
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.status",
        F.col("p.region").alias("holder_region"),
    )
)

with_windows = (
    joined_holder
    .select(
        "*",
        F.round(F.avg("premium").over(w_holder_id_product), 2).alias("holder_id_product_avg_premium"),
        F.count(F.lit(1)).over(w_holder_id_product).alias("rows_in_holder_id_product"),
        F.min("premium").over(w_holder_id_product).alias("holder_id_product_min_premium"),
        F.first("premium").over(w_all_by_end_date_full).alias("first_premium"),
        F.round(F.percent_rank().over(w_all_by_end_date_ties), 3).alias("end_date_percent_rank"),
        F.ntile(4).over(w_all_by_end_date).alias("end_date_quartile"),
        F.sum("premium").over(w_holder_id_product).alias("holder_id_product_sum_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_product), 2).alias("premium_vs_holder_id_product_avg"),
        F.dense_rank().over(w_all_by_end_date_ties).alias("end_date_dense_rank"),
    )
)

labeled = (
    with_windows
    .select(
        "id",
        "product",
        "premium",
        "status",
        "holder_id_product_avg_premium",
        "rows_in_holder_id_product",
        "holder_id_product_min_premium",
        "first_premium",
        "end_date_percent_rank",
        "end_date_quartile",
        "holder_id_product_sum_premium",
        "premium_vs_holder_id_product_avg",
        "end_date_dense_rank",
        F.round(F.col("premium") * 0.9, 2).alias("premium_adj"),
        F.greatest(F.col("end_date_dense_rank"), F.col("end_date_percent_rank")).alias("greatest_end_date_dense_rank_end_date_percent_ra"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

result = (
    labeled
    .select(
        "id",
        "premium",
        "status",
        "holder_id_product_avg_premium",
        "rows_in_holder_id_product",
        "first_premium",
        "end_date_percent_rank",
        "end_date_quartile",
        "holder_id_product_sum_premium",
        "premium_vs_holder_id_product_avg",
        "end_date_dense_rank",
        "premium_adj",
        "greatest_end_date_dense_rank_end_date_percent_ra",
        "product_or_unknown",
        F.initcap(F.trim(F.col("product_or_unknown"))).alias("product_or_unknown_clean"),
        F.round(F.col("premium_adj") * 1.07, 2).alias("premium_adj_adj"),
    )
)
