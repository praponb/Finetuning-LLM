from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status_by_premium = Window.partitionBy("holder_id", "status").orderBy("premium_bucket_per_premium")
w_holder_id_status_by_premium_2 = Window.partitionBy("holder_id", "status").orderBy("premium_bucket_per_premium", "id")
w_holder_id_status_by_premium_3 = Window.partitionBy("holder_id", "status").orderBy("premium_bucket_per_premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status = Window.partitionBy("status")
w_holder_id_status_by_premium_4 = Window.partitionBy("holder_id", "status").orderBy("premium_bucket_per_premium", "id").rowsBetween(-6, Window.currentRow)
w_status_by_product_len = Window.partitionBy("status").orderBy(F.col("product_len").desc_nulls_first(), "id")

recent = (
    df_policies
    .filter((F.col("premium") >= 1132.1) & (F.col("status") == "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.abs(F.col("premium") - 3465.4).alias("premium_distance"),
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
    )
)

qualified = (
    recent.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
)

tagged = (
    qualified
    .filter(F.col("status_prefix").isNotNull())
    .select(
        "id",
        "holder_id",
        "status",
        "premium_distance",
        "status_prefix",
        F.substring(F.trim(F.col("status_prefix")), 1, 2).alias("status_prefix_prefix"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.round(F.col("premium_bucket") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("premium_bucket_per_premium"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.round(F.cume_dist().over(w_holder_id_status_by_premium), 3).alias("premium_bucket_per_premium_cume_dist"),
        F.lag("product_len").over(w_holder_id_status_by_premium_2).alias("prev_product_len"),
        F.last("premium_bucket_per_premium").over(w_holder_id_status_by_premium_3).alias("last_premium_bucket_per_premium"),
        F.sum("premium_distance").over(w_status).alias("status_sum_premium_distance"),
        F.round(F.avg("product_len").over(w_holder_id_status_by_premium_4), 2).alias("product_len_avg_7"),
        F.lead("premium_bucket_per_premium", 2).over(w_status_by_product_len).alias("next2_premium_bucket_per_premium"),
    )
)

result = (
    windowed
    .groupingSets(
        [["product_len", "holder_id", "prev_product_len"], ["product_len"], ["holder_id"], []],
        "product_len", "holder_id", "prev_product_len",
    )
    .agg(
        F.count(F.when(F.col("status").isin("LAPSED", "ACTIVE", "EXPIRED"), 1)).alias("n_status_in_lapsed_active_expi"),
    )
    .orderBy(
        F.col("product_len").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
        F.col("prev_product_len").asc_nulls_last(),
    )
)
