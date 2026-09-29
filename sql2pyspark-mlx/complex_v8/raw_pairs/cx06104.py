from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_joined_on_month_month = Window.partitionBy(F.date_trunc("month", "joined_on_month"))
w_first_joined_on_year_by_medi = Window.partitionBy(F.year("first_joined_on")).orderBy(F.col("median_risk_score").desc(), "joined_on_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_known_joined_on_month = Window.partitionBy(F.to_date("first_known_joined_on_month"), "row_count").orderBy(F.col("rows_in_joined_on_month_month").asc_nulls_last(), "joined_on_month")
w_first_known_joined_on_month_2 = Window.partitionBy(F.to_date("first_known_joined_on_month"))

filtered = (
    df_policyholders
    .filter((F.col("risk_score") > 0.1) | (F.upper(F.trim(F.col("holder_name"))).like("D%")))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        F.date_format(F.col("joined_on"), "MMM yyyy").alias("joined_on_label"),
    )
)

summary = (
    filtered
    .filter(F.col("joined_on_label").isNotNull())
    .groupBy(F.date_trunc("month", "joined_on").alias("joined_on_month"))
    .agg(
        F.array_sort(F.collect_set("holder_name")).alias("holder_name_list"),
        F.min("joined_on").alias("first_joined_on"),
        F.sum("risk_score").alias("total_risk_score"),
        F.count("*").alias("row_count"),
        F.percentile_approx("risk_score", 0.5).alias("median_risk_score"),
    )
)

windowed = (
    summary
    .select(
        "joined_on_month",
        "holder_name_list",
        "first_joined_on",
        "total_risk_score",
        "row_count",
        F.sum("median_risk_score").over(w_joined_on_month_month).alias("joined_on_month_month_sum_median_risk_score"),
        F.count(F.lit(1)).over(w_joined_on_month_month).alias("rows_in_joined_on_month_month"),
        F.first("joined_on_month", ignorenulls=True).over(w_first_joined_on_year_by_medi).alias("first_known_joined_on_month"),
    )
)

metrics = (
    windowed
    .select(
        "joined_on_month",
        "first_joined_on",
        "total_risk_score",
        "joined_on_month_month_sum_median_risk_score",
        "rows_in_joined_on_month_month",
        "first_known_joined_on_month",
        F.row_number().over(w_first_known_joined_on_month).alias("seq_no"),
        F.max("total_risk_score").over(w_first_known_joined_on_month_2).alias("first_known_joined_on_month_to_date_max_total_ri"),
        F.round(F.col("total_risk_score") / F.nullif(F.sum("total_risk_score").over(w_first_known_joined_on_month_2), F.lit(0)), 4).alias("total_risk_score_share_of_first_known_joined_on_"),
        F.dense_rank().over(w_first_known_joined_on_month).alias("rows_in_joined_on_month_month_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "joined_on_month",
        "first_joined_on",
        "total_risk_score",
        "joined_on_month_month_sum_median_risk_score",
        "rows_in_joined_on_month_month",
        "first_known_joined_on_month",
        "seq_no",
        "first_known_joined_on_month_to_date_max_total_ri",
        "total_risk_score_share_of_first_known_joined_on_",
        "rows_in_joined_on_month_month_dense_rank",
        F.round(F.col("total_risk_score") * 0.9, 2).alias("total_risk_score_adj"),
    )
)
