from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_or_unknown_by_start_d = Window.partitionBy("status_or_unknown").orderBy(F.col("start_date").desc(), "id")
w_holder_id = Window.partitionBy("holder_id")
w_status_or_unknown = Window.partitionBy("status_or_unknown")
w_end_date_month_by_premium_pr = Window.partitionBy(F.month("end_date")).orderBy(F.col("premium").desc(), "id").rowsBetween(-2, -1)

filtered = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("premium") > 2111.3))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.when(F.col("status").isin("LAPSED", "EXPIRED"), "group_a").when(F.col("status") == "ACTIVE", "group_b").otherwise("other").alias("status_group"),
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status_or_unknown",
        (F.col("premium") - F.lag("premium").over(w_status_or_unknown_by_start_d)).alias("premium_change"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id), 2).alias("premium_vs_holder_id_avg"),
        F.round(F.avg("premium").over(w_holder_id), 2).alias("holder_id_avg_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id), F.lit(0)), 4).alias("premium_share_of_holder_id"),
    )
)

ranked = (
    sequenced
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status_or_unknown",
        "holder_id_avg_premium",
        "premium_share_of_holder_id",
        F.min("premium_share_of_holder_id").over(w_status_or_unknown).alias("status_or_unknown_min_premium_share_of_holder_id"),
        F.round(F.avg("premium_change").over(w_end_date_month_by_premium_pr), 2).alias("premium_change_avg_2_prior"),
    )
)

result = (
    ranked
    .groupingSets(
        [["product", "status_or_unknown"], ["product"], ["status_or_unknown"]],
        "product", "status_or_unknown",
    )
    .agg(
        F.grouping("product").alias("is_total_product"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.count("*").alias("cnt"),
    )
    .select(
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        F.coalesce(F.col("status_or_unknown"), F.lit("ALL")).alias("status_or_unknown"),
        "is_total_product",
        "product_list",
        "cnt",
    )
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("status_or_unknown").asc_nulls_last(),
    )
)
