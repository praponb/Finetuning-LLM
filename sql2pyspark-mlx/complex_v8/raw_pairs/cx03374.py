from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_status_by_start_date = Window.partitionBy("product", "status").orderBy("start_date", "id")
w_all_by_premium = Window.orderBy(F.col("premium").asc_nulls_last(), "id")
w_product_holder_id = Window.partitionBy("product", "holder_id")
w_all_by_premium_ties = Window.orderBy(F.col("premium").asc_nulls_last())
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")

scoped = (
    df_policies
    .filter(F.year(F.col("start_date")) == 2024)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_trunc("week", F.col("start_date")).alias("start_date_week"),
    )
)

with_details_holder = (
    scoped.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.joined_on") < F.lit("2025-07-03").cast("date"))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

windowed = (
    with_details_holder
    .select(
        "*",
        (F.col("premium") - F.lag("premium").over(w_product_status_by_start_date)).alias("premium_change"),
        F.ntile(4).over(w_all_by_premium).alias("premium_quartile"),
        F.round(F.avg("premium").over(w_product_holder_id), 2).alias("product_holder_id_avg_premium"),
        F.lead("holder_joined_on", 2).over(w_all_by_premium).alias("next2_holder_joined_on"),
        F.size(F.collect_set("status").over(w_product_holder_id)).alias("distinct_status_in_product_holder_id"),
        F.round(F.cume_dist().over(w_all_by_premium_ties), 3).alias("premium_cume_dist"),
        F.min("premium").over(w_product_holder_id).alias("product_holder_id_min_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product_holder_id), F.lit(0)), 4).alias("premium_share_of_product_holder_id"),
        F.round(F.cume_dist().over(w_all_by_start_date), 3).alias("start_date_cume_dist"),
        F.count(F.lit(1)).over(w_product_holder_id).alias("rows_in_product_holder_id"),
        F.lag("start_date", 2).over(w_all_by_start_date).alias("prev2_start_date"),
        F.max("premium").over(w_product_holder_id).alias("product_holder_id_max_premium"),
    )
)

stats = (
    windowed
    .groupBy(
        F.date_trunc("month", "next2_holder_joined_on").alias("next2_holder_joined_on_month"),
        "premium_quartile",
        "distinct_status_in_product_holder_id",
    )
    .agg(
        F.min("holder_joined_on").alias("first_holder_joined_on"),
        F.max("start_date").alias("last_start_date"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("start_date_cume_dist"), 2).alias("total_start_date_cume_dist"),
        F.max("product_holder_id_max_premium").alias("max_product_holder_id_max_premium"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    stats
    .select(
        "next2_holder_joined_on_month",
        "first_holder_joined_on",
        "distinct_ids",
        "total_start_date_cume_dist",
        "max_product_holder_id_max_premium",
        "row_count",
    )
    .orderBy(F.col("max_product_holder_id_max_premium").desc())
)
