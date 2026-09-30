from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium_prior_6 = Window.orderBy("premium", "id").rowsBetween(-6, -1)
w_start_date_month = Window.partitionBy(F.date_trunc("month", "start_date"))
w_status_product_len = Window.partitionBy("status", "product_len")
w_status_product_len_by_premiu = Window.partitionBy("status", "product_len").orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)

scoped = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.round(F.avg("premium").over(w_all_by_premium_prior_6), 2).alias("premium_avg_6_prior"),
        F.size(F.collect_set("status").over(w_start_date_month)).alias("distinct_status_in_start_date_month"),
        F.count(F.lit(1)).over(w_status_product_len).alias("rows_in_status_product_len"),
        F.count(F.lit(1)).over(w_start_date_month).alias("rows_in_start_date_month"),
        F.round(F.col("product_len") / F.nullif(F.sum("product_len").over(w_status_product_len), F.lit(0)), 4).alias("product_len_share_of_status_product_len"),
        F.size(F.collect_set("holder_id").over(w_status_product_len)).alias("distinct_holder_id_in_status_product_len"),
        F.round(F.col("premium_adj") - F.avg("premium_adj").over(w_status_product_len), 2).alias("premium_adj_vs_status_product_len_avg"),
        F.sum("premium_adj").over(w_status_product_len_by_premiu).alias("premium_adj_sum_4"),
    )
)

result = (
    metrics
    .select(
        "id",
        "premium",
        "start_date",
        "product_len",
        "premium_adj",
        "premium_avg_6_prior",
        "distinct_status_in_start_date_month",
        "rows_in_start_date_month",
        "product_len_share_of_status_product_len",
        "distinct_holder_id_in_status_product_len",
        "premium_adj_sum_4",
    )
    .orderBy(F.col("premium_adj").desc(), F.col("product_len").desc())
)
