from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_status_by_end_date_t = Window.partitionBy("product", "status").orderBy(F.col("end_date").desc_nulls_first())
w_product_status_by_end_date = Window.partitionBy("product", "status").orderBy(F.col("end_date").desc_nulls_first(), "id")
w_holder_id = Window.partitionBy("holder_id")

scoped = (
    df_policies
    .filter(F.col("product").isin("LIFE", "HOME"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.date_add(F.col("end_date"), 30).alias("end_date_plus_30d"),
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.round(F.percent_rank().over(w_product_status_by_end_date_t), 3).alias("end_date_percent_rank"),
        F.round(F.percent_rank().over(w_product_status_by_end_date), 3).alias("end_date_percent_rank_2"),
        F.min("premium").over(w_holder_id).alias("holder_id_min_premium"),
        F.datediff(F.col("end_date_plus_30d"), F.lag("end_date_plus_30d").over(w_product_status_by_end_date)).alias("days_since_prev_end_date_plus_30d"),
    )
)

screened = (
    ranked.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (~F.col("p.segment").isin("RETAIL", "VIP")) & (F.col("p.joined_on").isNotNull()), "left_anti")
)

result = (
    screened
    .cube("product", "status")
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(F.col("product").asc_nulls_last(), F.col("status").asc_nulls_last())
)
