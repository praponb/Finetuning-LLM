from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_joined_on_month_produ = Window.partitionBy(F.month("holder_joined_on"), "product").orderBy("premium_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_joined_on_month_produ_2 = Window.partitionBy(F.month("holder_joined_on"), "product").orderBy("premium_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_joined_on_month_start = Window.partitionBy(F.date_trunc("month", "holder_joined_on"), F.year("start_date")).orderBy(F.col("holder_joined_on").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_holder_joined_on_month_produ_3 = Window.partitionBy(F.month("holder_joined_on"), "product").orderBy("premium_bucket", "id").rowsBetween(-2, -1)
w_holder_joined_on_month_start_2 = Window.partitionBy(F.date_trunc("month", "holder_joined_on"), F.year("start_date")).orderBy(F.col("holder_joined_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_joined_on_month_start_3 = Window.partitionBy(F.date_trunc("month", "holder_joined_on"), F.year("start_date")).orderBy(F.col("holder_joined_on").asc_nulls_last(), "id")
w_holder_joined_on_month_produ_4 = Window.partitionBy(F.month("holder_joined_on"), "product").orderBy("premium_bucket")
w_start_date_month = Window.partitionBy(F.month("start_date"))
w_holder_joined_on_month_by_pr = Window.partitionBy(F.date_trunc("month", "holder_joined_on")).orderBy(F.col("premium_bucket_avg_2_prior").desc(), "id")
w_second_start_date_2_month = Window.partitionBy(F.month("second_start_date_2"))
w_holder_joined_on_month_by_pr_2 = Window.partitionBy(F.date_trunc("month", "holder_joined_on")).orderBy(F.col("premium_bucket_avg_2_prior").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_policys = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-08-27").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.when(F.col("status").isin("ACTIVE", "EXPIRED"), "group_a").when(F.col("status") == "LAPSED", "group_b").otherwise("other").alias("status_group"),
    )
)

joined_holder = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.segment") != "RETAIL"), "inner")
    .filter(F.col("p.risk_score").between(0.2, 0.9))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.premium_bucket",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
        "p.holder_name",
    )
)

sequenced = (
    joined_holder
    .select(
        "*",
        F.first("premium_bucket", ignorenulls=True).over(w_holder_joined_on_month_produ).alias("first_known_premium_bucket"),
        F.nth_value("start_date", 2).over(w_holder_joined_on_month_produ_2).alias("second_start_date"),
        F.round(F.avg("premium_bucket").over(w_holder_joined_on_month_start), 2).alias("premium_bucket_avg_6_prior"),
        F.sum("premium_bucket").over(w_holder_joined_on_month_produ).alias("running_premium_bucket"),
        F.round(F.avg("premium_bucket").over(w_holder_joined_on_month_produ_3), 2).alias("premium_bucket_avg_2_prior"),
        F.nth_value("start_date", 2).over(w_holder_joined_on_month_start_2).alias("second_start_date_2"),
        F.rank().over(w_holder_joined_on_month_start_3).alias("holder_joined_on_rank"),
        F.rank().over(w_holder_joined_on_month_produ_4).alias("premium_bucket_rank"),
        F.count(F.lit(1)).over(w_start_date_month).alias("rows_in_start_date_month"),
        F.sum("holder_risk_score").over(w_holder_joined_on_month_produ).alias("running_holder_risk_score"),
    )
)

top_per_group = (
    sequenced
    .filter((F.col("premium_bucket_rank") == 1) & (F.col("start_date").isNotNull()))
    .drop("premium_bucket_rank")
)

metrics = (
    top_per_group
    .select(
        "*",
        F.datediff(F.col("holder_joined_on"), F.lag("holder_joined_on").over(w_holder_joined_on_month_by_pr)).alias("days_since_prev_holder_joined_on"),
        F.count(F.lit(1)).over(w_second_start_date_2_month).alias("rows_in_second_start_date_2_month"),
        F.min("running_holder_risk_score").over(w_second_start_date_2_month).alias("second_start_date_2_month_min_running_holder_ris"),
        F.first("start_date", ignorenulls=True).over(w_holder_joined_on_month_by_pr_2).alias("first_known_start_date"),
        F.last("rows_in_start_date_month", ignorenulls=True).over(w_holder_joined_on_month_by_pr_2).alias("last_known_rows_in_start_date_month"),
    )
)

result = (
    metrics
    .select(
        "id",
        "holder_id",
        "product",
        "premium_bucket",
        "holder_joined_on",
        "holder_risk_score",
        "holder_name",
        "first_known_premium_bucket",
        "second_start_date",
        "premium_bucket_avg_6_prior",
        "running_premium_bucket",
        "premium_bucket_avg_2_prior",
        "rows_in_start_date_month",
        "running_holder_risk_score",
        "days_since_prev_holder_joined_on",
        "rows_in_second_start_date_2_month",
        "second_start_date_2_month_min_running_holder_ris",
        "last_known_rows_in_start_date_month",
    )
    .orderBy("id")
    .limit(20)
)
