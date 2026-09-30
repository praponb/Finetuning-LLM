from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_premium_ties = Window.partitionBy("product").orderBy("premium")
w_product_status = Window.partitionBy("product", "status")
w_all_by_end_date_ties = Window.orderBy(F.col("end_date").desc())
w_start_date_month_by_end_date = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy(F.col("end_date").desc(), "id")
w_product_by_premium = Window.partitionBy("product").orderBy("premium", "id")
w_start_date_month_by_end_date_2 = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_end_date_year = Window.partitionBy("holder_id", F.year("end_date"))
w_all_by_end_date_moving_1 = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_holder_segment = Window.partitionBy("holder_segment")
w_product_status_by_end_date = Window.partitionBy("product", "status").orderBy(F.col("end_date").desc(), "id")
w_all_by_start_date_ties = Window.orderBy(F.col("start_date").desc())
w_end_date_decile_by_start_dat = Window.partitionBy("end_date_decile").orderBy("start_date", "id").rowsBetween(-2, -1)
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_holder_segment_status = Window.partitionBy("holder_segment", "status")
w_distinct_products = Window.partitionBy("distinct_products")
w_all_by_max_premium_share_of = Window.orderBy(F.col("max_premium_share_of_holder_segment").desc_nulls_first(), "premium_dense_rank")

prepared_policys = (
    df_policies
    .filter((F.col("end_date").isNotNull()) & (~F.col("product").isin("AUTO", "HEALTH")))
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

enriched_holder = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.segment").alias("holder_segment"), "p.holder_name")
)

metrics = (
    enriched_holder
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "holder_segment",
        "holder_name",
        F.dense_rank().over(w_product_by_premium_ties).alias("premium_dense_rank"),
        F.size(F.collect_set("holder_id").over(w_product_status)).alias("distinct_holder_id_in_product_status"),
        F.rank().over(w_all_by_end_date_ties).alias("end_date_rank"),
        F.lag("start_date").over(w_start_date_month_by_end_date).alias("prev_start_date"),
        F.lead("premium", 3, 0).over(w_product_by_premium).alias("next3_premium"),
        F.last("premium", ignorenulls=True).over(w_start_date_month_by_end_date_2).alias("last_known_premium"),
        F.min("premium").over(w_holder_id_end_date_year).alias("holder_id_end_date_year_min_premium"),
        F.sum("premium").over(w_all_by_end_date_moving_1).alias("premium_sum_2"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_segment), 2).alias("premium_vs_holder_segment_avg"),
        F.count(F.lit(1)).over(w_product_status).alias("rows_in_product_status"),
        F.min("premium").over(w_holder_segment).alias("holder_segment_min_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_end_date_year), 2).alias("premium_vs_holder_id_end_date_year_avg"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_segment), F.lit(0)), 4).alias("premium_share_of_holder_segment"),
        F.ntile(10).over(w_product_status_by_end_date).alias("end_date_decile"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_end_date_year), F.lit(0)), 4).alias("premium_share_of_holder_id_end_date_year"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_start_date_ties), 3).alias("start_date_percent_rank"),
        F.round(F.avg("next3_premium").over(w_end_date_decile_by_start_dat), 2).alias("next3_premium_avg_2_prior"),
        F.ntile(3).over(w_all_by_start_date).alias("start_date_tercile"),
        F.round(F.col("rows_in_product_status") - F.avg("rows_in_product_status").over(w_holder_segment_status), 2).alias("rows_in_product_status_vs_holder_segment_status_"),
    )
)

per_group = (
    with_windows
    .groupBy("premium_dense_rank")
    .agg(
        F.round(F.sum("last_known_premium"), 2).alias("total_last_known_premium"),
        F.max("premium_share_of_holder_segment").alias("max_premium_share_of_holder_segment"),
        F.countDistinct("product").alias("distinct_products"),
    )
)

ranked = (
    per_group
    .withColumn("distinct_products_max_distinct_products", F.max("distinct_products").over(w_distinct_products))
    .withColumn("prev2_total_last_known_premium", F.lag("total_last_known_premium", 2, 0).over(w_all_by_max_premium_share_of))
)

result = (
    ranked
    .select(
        "premium_dense_rank",
        "total_last_known_premium",
        "max_premium_share_of_holder_segment",
        "distinct_products_max_distinct_products",
        "prev2_total_last_known_premium",
        F.when(F.col("total_last_known_premium") >= 18080.0, "large").when(F.col("total_last_known_premium") >= 10630.0, "medium").otherwise("small").alias("total_last_known_premium_tier"),
    )
)
