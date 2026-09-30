from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date = Window.orderBy("start_date", "id")
w_end_date_year_start_date_mon = Window.partitionBy("end_date_year", F.month("start_date")).orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_end_date_year_start_date_mon_2 = Window.partitionBy("end_date_year", F.month("start_date")).orderBy(F.col("premium").desc_nulls_first())

scoped_policys = (
    df_policies
    .filter(F.col("product") == "HEALTH")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.year(F.col("end_date")).alias("end_date_year"),
    )
)

derived = scoped_policys.withColumn("end_date_label", F.date_format(F.col("end_date"), "yyyy-MM-dd"))

with_windows = (
    derived
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "end_date_year",
        "end_date_label",
        F.lead("end_date", 3).over(w_all_by_start_date).alias("next3_end_date"),
        F.max("premium").over(w_end_date_year_start_date_mon).alias("premium_max_4"),
        F.round(F.percent_rank().over(w_end_date_year_start_date_mon_2), 3).alias("premium_percent_rank"),
    )
)

annotated = (
    with_windows
    .filter(F.col("start_date") < F.lit("2025-07-07").cast("date"))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "end_date_year",
        "next3_end_date",
        "premium_max_4",
        "premium_percent_rank",
        F.concat_ws("-", F.col("end_date_label"), F.col("product")).alias("end_date_label_product_label"),
        F.date_trunc("quarter", F.col("end_date")).alias("end_date_quarter"),
    )
)

derived_2 = (
    annotated
    .filter(F.year(F.col("start_date")) == 2025)
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date_quarter")).alias("days_since_end_date_quarter"),
        F.greatest(F.col("premium_percent_rank"), F.col("end_date_year")).alias("greatest_premium_percent_rank_end_date_year"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

annotated_2 = (
    derived_2
    .select(
        "id",
        "start_date",
        "end_date",
        "end_date_year",
        "premium_max_4",
        "premium_percent_rank",
        "end_date_label_product_label",
        "days_since_end_date_quarter",
        "greatest_premium_percent_rank_end_date_year",
        F.date_trunc("week", F.col("end_date")).alias("end_date_week"),
        F.datediff(F.col("next3_end_date"), F.col("start_date")).alias("days_start_date_to_next3_end_date"),
        F.date_trunc("hour", F.col("end_date_quarter")).alias("end_date_quarter_hour"),
    )
)

result = (
    annotated_2
    .select(
        "id",
        "end_date",
        "end_date_year",
        "premium_max_4",
        "premium_percent_rank",
        "end_date_label_product_label",
        "days_since_end_date_quarter",
        "greatest_premium_percent_rank_end_date_year",
        "end_date_week",
        "days_start_date_to_next3_end_date",
        "end_date_quarter_hour",
        F.coalesce(F.col("end_date_year"), F.lit(0)).alias("end_date_year_filled"),
    )
    .orderBy(F.col("greatest_premium_percent_rank_end_date_year").desc())
)
