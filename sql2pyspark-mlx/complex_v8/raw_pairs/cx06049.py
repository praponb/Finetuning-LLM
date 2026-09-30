from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_end_date_month_row_cou = Window.partitionBy(F.date_trunc("month", "first_end_date"), "row_count").orderBy("last_end_date")
w_first_end_date_month = Window.partitionBy(F.date_trunc("month", "first_end_date"))

base_policys = (
    df_policies
    .filter((F.col("status") == "LAPSED") | (F.col("start_date") >= F.lit("2024-08-27").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.concat_ws(": ", F.col("status"), F.col("product")).alias("status_product_label"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

qualified = (
    base_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type").isin("theft", "collision")) & (F.col("c.claim_date").isNotNull()), "left_semi")
    .filter(F.col("end_date") < F.lit("2026-11-25").cast("date"))
)

summary = (
    qualified
    .groupBy("status")
    .agg(
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.max("end_date").alias("last_end_date"),
        F.min("end_date").alias("first_end_date"),
        F.count("*").alias("row_count"),
        F.avg("days_start_date_to_end_date").alias("avg_days_start_date_to_end_date_check"),
    )
    .filter(F.col("avg_days_start_date_to_end_date_check") > -560)
    .drop("avg_days_start_date_to_end_date_check")
)

ranked = (
    summary
    .select(
        "*",
        F.round(F.cume_dist().over(w_first_end_date_month_row_cou), 3).alias("last_end_date_cume_dist"),
        F.round(F.percent_rank().over(w_first_end_date_month_row_cou), 3).alias("last_end_date_percent_rank"),
        F.max("row_count").over(w_first_end_date_month).alias("first_end_date_month_max_row_count"),
        F.round(F.avg("distinct_holder_ids").over(w_first_end_date_month), 2).alias("first_end_date_month_avg_distinct_holder_ids"),
    )
)

result = (
    ranked
    .select(
        "status",
        "first_end_date_month_avg_distinct_holder_ids",
        "last_end_date_percent_rank",
    )
    .unpivot(
        ids=["status"],
        values=["first_end_date_month_avg_distinct_holder_ids", "last_end_date_percent_rank"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("status", "field")
)
