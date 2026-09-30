from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_segment_by_pre = Window.partitionBy("status", "holder_segment").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_end_date_month_by_ho = Window.partitionBy("product", F.date_trunc("month", "end_date")).orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_end_date_month_by_ho_2 = Window.partitionBy("product", F.date_trunc("month", "end_date")).orderBy(F.col("holder_risk_score").desc(), "id")
w_status = Window.partitionBy("status")
w_holder_region_end_date_month = Window.partitionBy("holder_region", F.date_trunc("month", "end_date")).orderBy(F.col("end_date").desc(), "id").rowsBetween(-2, -1)
w_status_holder_segment_by_pre_2 = Window.partitionBy("status", "holder_segment").orderBy("premium", "id").rowsBetween(-2, -1)
w_status_holder_segment_by_pre_3 = Window.partitionBy("status", "holder_segment").orderBy("premium", "id").rowsBetween(-6, Window.currentRow)
w_product_end_date_month_by_ho_3 = Window.partitionBy("product", F.date_trunc("month", "end_date")).orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_region_end_date_month_2 = Window.partitionBy("holder_region", F.date_trunc("month", "end_date")).orderBy(F.col("end_date").desc(), "id")
w_holder_region = Window.partitionBy("holder_region")
w_all_by_row_count_ties = Window.orderBy("row_count")

scoped_policys = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("start_date") < F.lit("2025-05-28").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

enriched_holder = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.segment") == "SME")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.status",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
    )
)

metrics = (
    enriched_holder
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        "holder_region",
        "holder_segment",
        F.last("end_date", ignorenulls=True).over(w_status_holder_segment_by_pre).alias("last_known_end_date"),
        F.nth_value("holder_risk_score", 2).over(w_product_end_date_month_by_ho).alias("second_holder_risk_score"),
        (F.col("premium") - F.lag("premium").over(w_product_end_date_month_by_ho_2)).alias("premium_change"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.max("holder_risk_score").over(w_holder_region_end_date_month).alias("holder_risk_score_max_2_prior"),
        F.sum("holder_risk_score").over(w_status_holder_segment_by_pre_2).alias("holder_risk_score_sum_2_prior"),
        F.sum("holder_risk_score").over(w_status_holder_segment_by_pre_3).alias("holder_risk_score_sum_7"),
        F.last("holder_risk_score", ignorenulls=True).over(w_product_end_date_month_by_ho_3).alias("last_known_holder_risk_score"),
        F.lead("end_date", 3).over(w_holder_region_end_date_month_2).alias("next3_end_date"),
    )
)

tagged = (
    metrics
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "holder_region",
        "holder_segment",
        "second_holder_risk_score",
        "premium_change",
        "rows_in_status",
        "holder_risk_score_max_2_prior",
        "last_known_holder_risk_score",
        "next3_end_date",
        F.least(F.col("premium"), F.col("rows_in_status")).alias("least_premium_rows_in_status"),
    )
)

per_group = (
    tagged
    .groupBy("holder_region")
    .agg(
        F.max("end_date").alias("last_end_date"),
        F.percentile_approx("rows_in_status", 0.5).alias("median_rows_in_status"),
        F.count("*").alias("row_count"),
        F.sum("second_holder_risk_score").alias("total_second_holder_risk_score"),
        F.round(F.sum("least_premium_rows_in_status") / F.nullif(F.sum("rows_in_status"), F.lit(0)), 3).alias("least_premium_rows_in_status_to_rows_in_status_r"),
        F.min("least_premium_rows_in_status").alias("min_least_premium_rows_in_status"),
    )
)

ranked = (
    per_group
    .select(
        "holder_region",
        "median_rows_in_status",
        "row_count",
        "total_second_holder_risk_score",
        "least_premium_rows_in_status_to_rows_in_status_r",
        "min_least_premium_rows_in_status",
        F.round(F.col("median_rows_in_status") / F.nullif(F.sum("median_rows_in_status").over(w_holder_region), F.lit(0)), 4).alias("median_rows_in_status_share_of_holder_region"),
        F.count(F.lit(1)).over(w_holder_region).alias("rows_in_holder_region"),
        F.dense_rank().over(w_all_by_row_count_ties).alias("row_count_dense_rank"),
        F.max("row_count").over(w_holder_region).alias("holder_region_max_row_count"),
    )
)

result = (
    ranked
    .select(
        "holder_region",
        "median_rows_in_status",
        "least_premium_rows_in_status_to_rows_in_status_r",
        "rows_in_holder_region",
        "row_count_dense_rank",
        "holder_region_max_row_count",
    )
    .orderBy(
        F.col("least_premium_rows_in_status_to_rows_in_status_r").asc(),
        "holder_region",
    )
    .limit(5)
)
