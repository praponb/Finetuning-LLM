from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_holder_risk_score = Window.partitionBy("product").orderBy("holder_risk_score", "id")
w_holder_id_product = Window.partitionBy("holder_id", "product")
w_product = Window.partitionBy("product")
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id")
w_all_by_premium_ties = Window.orderBy(F.col("premium").desc())
w_all_by_premium = Window.orderBy(F.col("premium").desc(), "id")
w_holder_id_end_date_month_by = Window.partitionBy("holder_id", F.date_trunc("month", "end_date")).orderBy("end_date", "id")
w_holder_id_by_premium_share_o = Window.partitionBy("holder_id").orderBy(F.col("premium_share_of_product").desc(), "id")
w_premium_rank_by_next_end_dat = Window.partitionBy("premium_rank").orderBy("next_end_date", "id")
w_premium_rank_by_next_end_dat_2 = Window.partitionBy("premium_rank").orderBy("next_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_rows_in_product_runni = Window.orderBy("rows_in_product", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_rows_in_product_ties = Window.orderBy("rows_in_product")

prepared = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-02-14").cast("date")) & (F.col("holder_id") <= 18))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

enriched = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", "p.holder_name", F.col("p.risk_score").alias("holder_risk_score"))
)

metrics = (
    enriched
    .select(
        "*",
        F.row_number().over(w_product_by_holder_risk_score).alias("holder_risk_score_seq"),
        F.count(F.lit(1)).over(w_holder_id_product).alias("rows_in_holder_id_product"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.lead("holder_risk_score", 1, 0).over(w_holder_id_by_premium).alias("next_holder_risk_score"),
        F.round(F.avg("premium").over(w_product), 2).alias("product_avg_premium"),
        F.round(F.percent_rank().over(w_all_by_premium_ties), 3).alias("premium_percent_rank"),
        F.lead("end_date").over(w_all_by_premium).alias("next_end_date"),
        F.rank().over(w_all_by_premium_ties).alias("premium_rank"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.round(F.cume_dist().over(w_holder_id_by_premium), 3).alias("premium_cume_dist"),
        F.rank().over(w_holder_id_end_date_month_by).alias("end_date_rank"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.row_number().over(w_holder_id_by_premium_share_o).alias("seq_no"),
        F.ntile(4).over(w_premium_rank_by_next_end_dat).alias("next_end_date_quartile"),
        F.first("end_date", ignorenulls=True).over(w_premium_rank_by_next_end_dat_2).alias("first_known_end_date"),
        F.sum("premium_rank").over(w_all_by_rows_in_product_runni).alias("running_premium_rank"),
        F.rank().over(w_all_by_rows_in_product_ties).alias("rows_in_product_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("premium") > 1042.5)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status_ref",
        "holder_name",
        "holder_risk_score_seq",
        "premium_share_of_product",
        "next_holder_risk_score",
        "product_avg_premium",
        "premium_rank",
        "rows_in_product",
        "end_date_rank",
        "seq_no",
        "next_end_date_quartile",
        "first_known_end_date",
        "running_premium_rank",
    )
    .orderBy("id", F.col("next_end_date_quartile").desc_nulls_first())
    .limit(20)
)
