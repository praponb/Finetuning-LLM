from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status_or_unknown = Window.partitionBy("holder_id", "status_or_unknown")
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy(F.col("premium_adj").desc(), "id")

recent_policys = (
    df_policies
    .filter((F.col("status") != "LAPSED") & (F.col("end_date") >= F.lit("2025-09-27").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

product_stats = (
    recent_policys
    .groupBy("product")
    .agg(F.avg("premium_adj").alias("avg_premium_adj"))
)

compared = (
    recent_policys.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.premium_adj") > F.col("g.avg_premium_adj"))
    .select(
        "s.*",
        "g.avg_premium_adj",
        F.round(F.col("s.premium_adj") - F.col("g.avg_premium_adj"), 2).alias("premium_adj_minus_avg"),
    )
)

with_trends = (
    compared
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "status_slug",
        "status_or_unknown",
        "avg_premium_adj",
        F.count(F.lit(1)).over(w_holder_id_status_or_unknown).alias("rows_in_holder_id_status_or_unknown"),
        F.lag("avg_premium_adj", 3).over(w_product_holder_id_by_premium).alias("prev3_avg_premium_adj"),
    )
)

totals = (
    with_trends
    .filter(F.col("status_or_unknown").isNotNull())
    .groupBy("status_or_unknown", "holder_id")
    .agg(
        F.max("prev3_avg_premium_adj").alias("max_prev3_avg_premium_adj"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("n_status_cancelled"),
        F.round(F.avg("rows_in_holder_id_status_or_unknown"), 2).alias("avg_rows_in_holder_id_status_or_unknown"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("avg_premium_adj"), 2).alias("stddev_avg_premium_adj"),
    )
)

with_parent = (
    totals.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.segment").isNotNull())
    .select(
        "s.*",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

result = (
    with_parent
    .groupingSets(
        [["status_or_unknown", "n_status_cancelled", "holder_id"], ["status_or_unknown"], []],
        "status_or_unknown", "n_status_cancelled", "holder_id",
    )
    .agg(
        F.round(F.sum("avg_rows_in_holder_id_status_or_unknown") / F.nullif(F.sum("max_prev3_avg_premium_adj"), F.lit(0)), 3).alias("avg_rows_in_holder_id_status_or_unknown_to_max_p"),
        F.round(F.avg("avg_rows_in_holder_id_status_or_unknown"), 2).alias("avg_avg_rows_in_holder_id_status_or_unknown"),
    )
    .orderBy(
        F.col("status_or_unknown").asc_nulls_last(),
        F.col("n_status_cancelled").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
