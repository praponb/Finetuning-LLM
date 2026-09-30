from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_lower_holder_name_like_er = Window.partitionBy("n_lower_holder_name_like_er", "holder_region")
w_n_rows_product_by_n_rows_run = Window.partitionBy("n_rows", "product").orderBy(F.col("n_rows").desc_nulls_first(), "start_date_month", "product", "holder_region").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_rows_product_by_n_rows_tie = Window.partitionBy("n_rows", "product").orderBy(F.col("n_rows").desc_nulls_first())

recent_policys = (
    df_policies
    .filter(F.col("end_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

enriched_holder = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.status",
        F.col("p.region").alias("holder_region"),
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
    )
)

stats = (
    enriched_holder
    .groupBy(
        F.date_trunc("month", "start_date").alias("start_date_month"),
        "product",
        "holder_region",
    )
    .agg(
        F.min("start_date").alias("first_start_date"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.lower(F.col("holder_name")).like("%er%"), 1).otherwise(0)).alias("n_lower_holder_name_like_er"),
        F.count("*").alias("n_rows"),
        F.countDistinct("status").alias("distinct_status"),
        F.count(F.when(F.upper(F.trim(F.col("holder_name"))).like("D%"), 1)).alias("n_upper_trim_holder_name_like"),
    )
)

filtered = (
    stats
    .crossJoin(stats.agg(F.avg("n_upper_trim_holder_name_like").alias("overall_avg_n_upper_trim_holder_name_like")))
    .filter(F.col("n_upper_trim_holder_name_like") > F.col("overall_avg_n_upper_trim_holder_name_like"))
    .drop("overall_avg_n_upper_trim_holder_name_like")
)

sequenced = (
    filtered
    .select(
        "start_date_month",
        "product",
        "holder_region",
        "status_list",
        "n_lower_holder_name_like_er",
        "n_rows",
        "distinct_status",
        "n_upper_trim_holder_name_like",
        F.count(F.lit(1)).over(w_n_lower_holder_name_like_er).alias("rows_in_n_lower_holder_name_like_er_holder_regio"),
        F.max("n_rows").over(w_n_lower_holder_name_like_er).alias("n_lower_holder_name_like_er_holder_region_max_n_"),
        F.first("n_lower_holder_name_like_er", ignorenulls=True).over(w_n_rows_product_by_n_rows_run).alias("first_known_n_lower_holder_name_like_er"),
        F.rank().over(w_n_rows_product_by_n_rows_tie).alias("n_rows_rank"),
    )
)

result = (
    sequenced
    .select(
        "start_date_month",
        "product",
        "holder_region",
        "status_list",
        "n_lower_holder_name_like_er",
        "n_rows",
        "distinct_status",
        "first_known_n_lower_holder_name_like_er",
        (F.col("n_rows_rank") + F.col("n_rows")).alias("n_rows_rank_plus_n_rows"),
    )
    .orderBy("product")
)
