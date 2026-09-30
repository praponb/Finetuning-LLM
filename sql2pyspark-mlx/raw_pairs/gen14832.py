from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id", "store_id")
w4 = Window.orderBy("dedup_rank", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("product_id", "store_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc_nulls_last())
w7 = Window.orderBy(F.col("corr_units_revenue").desc(), "id")
w8 = Window.orderBy(F.col("revenue_share").desc(), "id")
w9 = Window.orderBy("revenue_share", "id")
w10 = Window.orderBy("last_known_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy(F.col("corr_units_revenue").desc_nulls_last())
w_grp = Window.orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy("revenue_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.orderBy("revenue_rank", "id")
w15 = Window.orderBy(F.col("row_seq").desc(), "id")
w16 = Window.orderBy("last_known_units", "id")

ranked = (
    df_store_sales
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "sale_date",
        "units",
        "dedup_rank",
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4).alias("revenue_share"),
        F.corr("units", "revenue").over(w3).alias("corr_units_revenue"),
        F.avg("revenue").over(w4).alias("revenue_avg_2_prior"),
        F.last("units", ignorenulls=True).over(w5).alias("last_known_units"),
        F.rank().over(w6).alias("revenue_rank"),
    )
)

metrics_store_sales = (
    metrics
    .filter(F.col("last_known_units") == 24)
    .select(
        "*",
        F.row_number().over(w7).alias("row_seq"),
        F.ntile(5).over(w8).alias("revenue_share_bucket"),
        F.ntile(5).over(w9).alias("revenue_share_bucket_2"),
        F.sum("revenue_rank").over(w10).alias("running_revenue_rank"),
        F.percent_rank().over(w11).alias("corr_units_revenue_percent_rank"),
    )
)

scored = (
    metrics_store_sales
    .select(
        "*",
        F.sum("revenue_share_bucket_2").over(w_grp).alias("running_revenue_share_bucket_2"),
        F.last("row_seq", ignorenulls=True).over(w13).alias("last_known_row_seq"),
        F.lead("revenue_share_bucket", 1).over(w_grp_2).alias("next_revenue_share_bucket"),
        F.ntile(5).over(w15).alias("row_seq_bucket"),
        F.row_number().over(w16).alias("seq_no"),
    )
)

totals = (
    scored
    .groupBy(
        "seq_no",
        F.when(F.col("next_revenue_share_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("next_revenue_share_bucket_tier"),
    )
    .agg(
        F.percentile_approx("last_known_units", 0.5).alias("last_known_units_p50"),
        F.percentile_approx("revenue_share_bucket", 0.9).alias("revenue_share_bucket_p90"),
        F.percentile_approx("revenue_share", 0.9).alias("revenue_share_p90"),
        F.sum("running_revenue_share_bucket_2").alias("total_running_revenue_share_bucket_2"),
    )
)

result = (
    totals
    .select(
        "seq_no",
        "next_revenue_share_bucket_tier",
        "last_known_units_p50",
        "revenue_share_p90",
    )
)
