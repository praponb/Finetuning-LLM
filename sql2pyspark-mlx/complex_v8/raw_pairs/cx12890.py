from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_year_premium_tier = Window.partitionBy(F.year("start_date"), "premium_tier")
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy("premium", "id")
w_status_start_date_month_by_s = Window.partitionBy("status", F.date_trunc("month", "start_date")).orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_premium_tier_by_fi = Window.partitionBy("holder_id", "premium_tier").orderBy("first_known_start_date", "id")
w_holder_id = Window.partitionBy("holder_id")
w_holder_id_premium_tier_by_fi_2 = Window.partitionBy("holder_id", "premium_tier").orderBy("first_known_start_date")

scoped = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.when(F.col("premium") >= 3521.8, "large").when(F.col("premium") >= 2289.0, "medium").otherwise("small").alias("premium_tier"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.size(F.collect_set("status").over(w_start_date_year_premium_tier)).alias("distinct_status_in_start_date_year_premium_tier"),
        F.ntile(4).over(w_holder_id_by_premium).alias("premium_quartile"),
        F.sum("premium").over(w_start_date_year_premium_tier).alias("start_date_year_premium_tier_sum_premium"),
        F.min("premium").over(w_start_date_year_premium_tier).alias("start_date_year_premium_tier_min_premium"),
        F.first("start_date", ignorenulls=True).over(w_status_start_date_month_by_s).alias("first_known_start_date"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.round(F.percent_rank().over(w_holder_id_premium_tier_by_fi), 3).alias("first_known_start_date_percent_rank"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.size(F.collect_set("premium_tier").over(w_holder_id)).alias("distinct_premium_tier_in_holder_id"),
        F.rank().over(w_holder_id_premium_tier_by_fi_2).alias("first_known_start_date_rank"),
    )
)

enriched = (
    windowed
    .select(
        "*",
        F.concat_ws("-", F.col("status"), F.col("premium_tier")).alias("status_premium_tier_label"),
        F.date_trunc("quarter", F.col("start_date")).alias("start_date_quarter"),
    )
)

result = (
    enriched
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        "premium_tier",
        "distinct_status_in_start_date_year_premium_tier",
        "start_date_year_premium_tier_sum_premium",
        "start_date_year_premium_tier_min_premium",
        "first_known_start_date",
        "rows_in_holder_id",
        "first_known_start_date_rank",
        "status_premium_tier_label",
        "start_date_quarter",
        F.least(F.col("premium_quartile"), F.col("premium")).alias("least_premium_quartile_premium"),
    )
    .orderBy(
        "least_premium_quartile_premium",
        F.col("first_known_start_date").desc(),
    )
)
