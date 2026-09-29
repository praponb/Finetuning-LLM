from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_all_by_premium_ties = Window.orderBy("premium")
w_premium_dense_rank = Window.partitionBy("premium_dense_rank")
w_cnt_n_product_is_not_null_by = Window.partitionBy("cnt", "n_product_is_not_null").orderBy(F.col("total_premium").desc(), "start_date_month", "holder_id", "premium_dense_rank").rowsBetween(-2, Window.currentRow)

filtered = (
    df_policies
    .filter(F.col("premium").between(1051.9, 3544.1))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
    )
)

with_trends = (
    filtered
    .withColumn("rows_in_product", F.count(F.lit(1)).over(w_product))
    .withColumn("premium_dense_rank", F.dense_rank().over(w_all_by_premium_ties))
)

labeled = with_trends.select("*", F.round(F.col("premium") * 0.9, 2).alias("premium_adj"))

scored = (
    labeled
    .filter(F.col("product") == "HOME")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "premium_dense_rank",
        "premium_adj",
        F.round(F.col("rows_in_product") / F.nullif(F.col("premium_adj"), F.lit(0)), 3).alias("rows_in_product_per_premium_adj"),
        (F.floor(F.col("premium_adj") / 1000) * 1000).alias("premium_adj_bucket"),
    )
)

totals = (
    scored
    .groupBy(
        F.date_trunc("month", "start_date").alias("start_date_month"),
        "holder_id",
        "premium_dense_rank",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(100 * F.count(F.when(F.col("product").isin("TRAVEL", "AUTO"), 1)) / F.count("*"), 1).alias("pct_product_in_travel_auto"),
        F.min("start_date").alias("first_start_date"),
        F.count(F.when(F.col("product").isNotNull(), 1)).alias("n_product_is_not_null"),
        F.sum("premium").alias("total_premium"),
    )
)

with_trends_2 = (
    totals
    .select(
        "*",
        F.size(F.collect_set("holder_id").over(w_premium_dense_rank)).alias("distinct_holder_id_in_premium_dense_rank"),
        F.sum("premium_dense_rank").over(w_cnt_n_product_is_not_null_by).alias("premium_dense_rank_sum_3"),
        F.sum("cnt").over(w_premium_dense_rank).alias("premium_dense_rank_sum_cnt"),
    )
)

result = (
    with_trends_2
    .groupingSets(
        [["n_product_is_not_null", "cnt"], ["n_product_is_not_null"], ["cnt"], []],
        "n_product_is_not_null", "cnt",
    )
    .agg(
        F.datediff(F.max("first_start_date"), F.min("first_start_date")).alias("first_start_date_span_days"),
        F.count("*").alias("row_count"),
        F.round(F.avg("distinct_holder_id_in_premium_dense_rank"), 2).alias("avg_distinct_holder_id_in_premium_dense_rank"),
    )
    .orderBy(
        F.col("n_product_is_not_null").asc_nulls_last(),
        F.col("cnt").asc_nulls_last(),
    )
)
