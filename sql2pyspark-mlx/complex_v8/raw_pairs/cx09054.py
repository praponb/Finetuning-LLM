from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date = Window.orderBy("end_date", "id")
w_product_start_date_year = Window.partitionBy("product", F.year("start_date"))
w_all_by_end_date_full = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_policies
    .filter((F.col("status").isNotNull()) & (F.col("start_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("status").isin("ACTIVE", "EXPIRED"), "group_a").when(F.col("status") == "LAPSED", "group_b").otherwise("other").alias("status_group"),
    )
)

screened = (
    prepared.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id").isNotNull()), "left_semi")
    .filter(F.col("premium").between(1541.8, 4688.7))
)

holder_id_stats = (
    screened
    .groupBy("holder_id")
    .agg(F.max("premium").alias("max_premium"))
)

outliers = (
    screened.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium") == F.col("g.max_premium"))
    .select(
        "s.*",
        "g.max_premium",
        F.round(F.col("s.premium") - F.col("g.max_premium"), 2).alias("premium_minus_max"),
    )
)

metrics = (
    outliers
    .select(
        "*",
        F.count(F.lit(1)).over(w_all_by_end_date_running).alias("running_count"),
        F.dense_rank().over(w_all_by_end_date).alias("end_date_dense_rank"),
        F.lag("premium", 2).over(w_all_by_end_date).alias("prev2_premium"),
        F.rank().over(w_all_by_end_date).alias("end_date_rank"),
        F.round(F.col("premium_minus_max") / F.nullif(F.sum("premium_minus_max").over(w_product_start_date_year), F.lit(0)), 4).alias("premium_minus_max_share_of_product_start_date_ye"),
        F.nth_value("start_date", 2).over(w_all_by_end_date_full).alias("second_start_date"),
    )
)

result = (
    metrics
    .cube("end_date_dense_rank", "status_group", "holder_id")
    .agg(F.min("end_date").alias("first_end_date"))
    .select(
        "end_date_dense_rank",
        F.coalesce(F.col("status_group"), F.lit("ALL")).alias("status_group"),
        "holder_id",
        "first_end_date",
    )
)
