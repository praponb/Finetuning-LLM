from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_start_date_runn = Window.partitionBy("holder_id").orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_month = Window.partitionBy(F.date_trunc("month", "start_date"))
w_end_date_year_by_premium_sha = Window.partitionBy(F.year("end_date")).orderBy("premium_share_of_start_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_year_by_premium_sha_2 = Window.partitionBy(F.year("end_date")).orderBy("premium_share_of_start_date_month", "id")
w_start_date_year_holder_id_by = Window.partitionBy(F.year("start_date"), "holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id = Window.partitionBy("holder_id")
w_end_date_year_by_premium_sha_3 = Window.partitionBy(F.year("end_date")).orderBy("premium_share_of_start_date_month")

valid_policys = (
    df_policies
    .filter((F.col("product").isin("TRAVEL", "AUTO", "LIFE")) & (F.col("start_date") >= F.lit("2024-11-20").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
    )
)

screened = (
    valid_policys.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") >= 1132.1) & (F.year(F.col("p.end_date")) == 2026), "left_semi")
    .filter(F.col("start_date") >= F.lit("2024-10-22").cast("date"))
)

ranked = (
    screened
    .select(
        "*",
        F.sum("premium").over(w_holder_id_by_start_date_runn).alias("running_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_start_date_month), F.lit(0)), 4).alias("premium_share_of_start_date_month"),
        F.count(F.lit(1)).over(w_start_date_month).alias("rows_in_start_date_month"),
    )
)

sequenced = (
    ranked
    .select(
        "*",
        F.min("rows_in_start_date_month").over(w_end_date_year_by_premium_sha).alias("min_rows_in_start_date_month_so_far"),
        F.row_number().over(w_end_date_year_by_premium_sha_2).alias("premium_share_of_start_date_month_seq"),
        F.last("end_date", ignorenulls=True).over(w_start_date_year_holder_id_by).alias("last_known_end_date"),
        F.round(F.avg("rows_in_start_date_month").over(w_holder_id), 2).alias("holder_id_avg_rows_in_start_date_month"),
        F.dense_rank().over(w_end_date_year_by_premium_sha_3).alias("premium_share_of_start_date_month_dense_rank"),
    )
)

enriched = sequenced.select("*", F.month(F.col("end_date")).alias("end_date_month"))

with_parent = (
    enriched.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.running_premium",
        "s.premium_share_of_start_date_month",
        "s.rows_in_start_date_month",
        "s.holder_id_avg_rows_in_start_date_month",
        "s.premium_share_of_start_date_month_dense_rank",
        "s.end_date_month",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
        "p.holder_name",
    )
)

result = (
    with_parent
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "running_premium",
        "premium_share_of_start_date_month",
        "rows_in_start_date_month",
        "holder_id_avg_rows_in_start_date_month",
        "premium_share_of_start_date_month_dense_rank",
        "end_date_month",
        "holder_risk_score",
        "holder_region",
        "holder_name",
        F.length(F.trim(F.col("holder_name"))).alias("holder_name_len"),
    )
    .orderBy(F.col("running_premium").asc_nulls_last())
)
