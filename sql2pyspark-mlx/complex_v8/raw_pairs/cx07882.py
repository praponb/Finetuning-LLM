from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_quarter_status = Window.partitionBy("end_date_quarter", "status")
w_status_by_end_date_quarter_m = Window.partitionBy("status").orderBy("end_date_quarter", "id").rowsBetween(-1, Window.currentRow)
w_status_by_end_date_quarter_f = Window.partitionBy("status").orderBy("end_date_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_end_date_quarter = Window.partitionBy("status").orderBy("end_date_quarter", "id")

valid = (
    df_policies
    .filter((F.col("product") == "TRAVEL") & (F.col("status") == "EXPIRED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.quarter(F.col("end_date")).alias("end_date_quarter"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.size(F.collect_set("holder_id").over(w_end_date_quarter_status)).alias("distinct_holder_id_in_end_date_quarter_status"),
        F.round(F.avg("premium_bucket").over(w_status_by_end_date_quarter_m), 2).alias("premium_bucket_avg_2"),
        F.size(F.collect_set("product").over(w_end_date_quarter_status)).alias("distinct_product_in_end_date_quarter_status"),
        F.first("premium").over(w_status_by_end_date_quarter_f).alias("first_premium"),
        F.min("premium_bucket").over(w_end_date_quarter_status).alias("end_date_quarter_status_min_premium_bucket"),
        F.round(F.percent_rank().over(w_status_by_end_date_quarter), 3).alias("end_date_quarter_percent_rank"),
    )
)

enriched_holder = (
    windowed.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.status",
        "s.premium_bucket",
        "s.end_date_quarter",
        "s.distinct_holder_id_in_end_date_quarter_status",
        "s.premium_bucket_avg_2",
        "s.distinct_product_in_end_date_quarter_status",
        "s.first_premium",
        "s.end_date_quarter_status_min_premium_bucket",
        "s.end_date_quarter_percent_rank",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
        "p.holder_name",
    )
)

product_stats = (
    enriched_holder
    .groupBy("product")
    .agg(F.avg("premium_bucket_avg_2").alias("avg_premium_bucket_avg_2"))
)

above_average = (
    enriched_holder.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.premium_bucket_avg_2") > F.col("g.avg_premium_bucket_avg_2"))
    .select(
        "s.*",
        "g.avg_premium_bucket_avg_2",
        F.round(F.col("s.premium_bucket_avg_2") - F.col("g.avg_premium_bucket_avg_2"), 2).alias("premium_bucket_avg_2_minus_avg"),
    )
)

result = (
    above_average
    .groupingSets(
        [["distinct_product_in_end_date_quarter_status", "holder_id"], ["distinct_product_in_end_date_quarter_status"], []],
        "distinct_product_in_end_date_quarter_status", "holder_id",
    )
    .agg(
        F.grouping("distinct_product_in_end_date_quarter_status").alias("is_total_distinct_product_in_end_date_quarter_st"),
        F.round(F.stddev("premium_bucket_avg_2_minus_avg"), 2).alias("stddev_premium_bucket_avg_2_minus_avg"),
    )
    .orderBy(
        F.col("distinct_product_in_end_date_quarter_status").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
