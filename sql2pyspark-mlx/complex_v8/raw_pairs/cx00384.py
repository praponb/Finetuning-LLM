from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_row_count = Window.partitionBy("row_count")
w_row_count_by_row_count_ties = Window.partitionBy("row_count").orderBy("row_count")
w_row_count_by_row_count_runni = Window.partitionBy("row_count").orderBy("row_count", "claim_date_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_known_claim_date_year = Window.partitionBy("first_known_claim_date_year", "claim_date_year").orderBy("first_known_claim_date_year", "claim_date_year")
w_all_by_total_approved_amount = Window.orderBy("total_approved_amount", "claim_date_year")
w_first_known_claim_date_year_2 = Window.partitionBy("first_known_claim_date_year", "claim_date_year").orderBy("first_known_claim_date_year", "claim_date_year").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_year = Window.partitionBy("claim_date_year")
w_all_by_rows_in_row_count_tie = Window.orderBy("rows_in_row_count")

valid_claims = (
    df_claims
    .filter(F.col("adjuster_id") <= 5)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.quarter(F.col("reported_at")).alias("reported_at_quarter"),
        F.lower(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

totals = (
    valid_claims
    .groupBy(F.year("claim_date").alias("claim_date_year"))
    .agg(
        F.sum("approved_amount").alias("total_approved_amount"),
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
    )
)

with_activity = (
    totals
    .crossJoin(totals.agg(F.min("total_approved_amount").alias("overall_min_total_approved_amount")))
    .filter(F.col("total_approved_amount") > F.col("overall_min_total_approved_amount"))
    .drop("overall_min_total_approved_amount")
)

with_windows = (
    with_activity
    .select(
        "*",
        F.min("total_approved_amount").over(w_row_count).alias("row_count_min_total_approved_amount"),
        F.min("claim_date_year").over(w_row_count).alias("row_count_min_claim_date_year"),
        F.dense_rank().over(w_row_count_by_row_count_ties).alias("row_count_dense_rank"),
        F.count(F.lit(1)).over(w_row_count).alias("rows_in_row_count"),
        F.first("claim_date_year", ignorenulls=True).over(w_row_count_by_row_count_runni).alias("first_known_claim_date_year"),
    )
)

first_rows = (
    with_windows
    .filter(F.col("row_count_dense_rank") <= 5)
    .drop("row_count_dense_rank")
)

windowed = (
    first_rows
    .select(
        "*",
        F.lead("total_approved_amount").over(w_first_known_claim_date_year).alias("next_total_approved_amount"),
        F.ntile(3).over(w_all_by_total_approved_amount).alias("total_approved_amount_tercile"),
        F.last("total_approved_amount").over(w_first_known_claim_date_year_2).alias("last_total_approved_amount"),
        F.round(F.col("row_count") / F.nullif(F.sum("row_count").over(w_claim_date_year), F.lit(0)), 4).alias("row_count_share_of_claim_date_year"),
        F.round(F.cume_dist().over(w_all_by_rows_in_row_count_tie), 3).alias("rows_in_row_count_cume_dist"),
        F.round(F.col("total_approved_amount") / F.nullif(F.sum("total_approved_amount").over(w_claim_date_year), F.lit(0)), 4).alias("total_approved_amount_share_of_claim_date_year"),
    )
)

result = (
    windowed
    .select("claim_date_year", "rows_in_row_count_cume_dist", "total_approved_amount")
    .unpivot(
        ids=["claim_date_year"],
        values=["rows_in_row_count_cume_dist", "total_approved_amount"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("claim_date_year", "field")
)
