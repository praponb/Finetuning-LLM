from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "end_date"), "plan").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("MONTH", "start_date"), "plan").orderBy("end_date", "id")
w3 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())
w_grp = Window.partitionBy("customer_id").orderBy(F.col("row_seq").desc(), "id")
w5 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_percent_rank").desc_nulls_first(), "id")

enriched = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.signup_date"))
)

enriched_subscriptions = (
    enriched
    .select(
        "*",
        F.sum("monthly_fee").over(w).alias("cumulative_monthly_fee"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

enriched_subscriptions_2 = (
    enriched_subscriptions
    .filter(F.col("end_date").between("2025-05-04", "2025-06-24"))
    .withColumn("monthly_fee_percent_rank", F.percent_rank().over(w3))
    .withColumn("row_seq_2", F.row_number().over(w_grp))
)

deduped = (
    enriched_subscriptions_2
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select("id", "status", "monthly_fee_percent_rank", "monthly_fee")
    .unpivot(
        ids=["id", "status"],
        values=["monthly_fee_percent_rank", "monthly_fee"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
