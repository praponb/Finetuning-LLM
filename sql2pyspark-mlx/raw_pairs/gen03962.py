from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc(), "id")
w3 = Window.partitionBy(F.month("end_date"), "plan")
w4 = Window.partitionBy("segment", "customer_id").orderBy("monthly_fee", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.partitionBy("customer_id", "plan").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.year("end_date")).orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("plan", "segment").orderBy(F.col("running_monthly_fee").desc(), "id")
w9 = Window.partitionBy("plan").orderBy("running_monthly_fee")
w10 = Window.partitionBy("plan").orderBy("dedup_rank", "id")
w11 = Window.partitionBy("segment").orderBy("monthly_fee_avg_2", "id")
w_ordered = Window.orderBy("last_known_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy(F.col("latest_rank_2").desc(), "id")
w_grp = Window.partitionBy("plan").orderBy("running_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_subscriptions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

with_dims = (
    latest
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.plan",
        "a.end_date",
        "a.monthly_fee",
        "a.rn",
        F.col("d.credit_limit"),
        F.col("d.segment"),
    )
)

metrics = (
    with_dims
    .filter(F.col("credit_limit") >= 6300)
    .select(
        "id",
        "plan",
        "segment",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4).alias("monthly_fee_share"),
        F.avg("monthly_fee").over(w4).alias("monthly_fee_avg_2"),
        F.sum("monthly_fee").over(w_recent).alias("running_monthly_fee"),
        F.last("segment", ignorenulls=True).over(w6).alias("last_known_segment"),
        F.last("credit_limit", ignorenulls=True).over(w7).alias("last_known_credit_limit"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

ranked_subscriptions = (
    ranked
    .withColumn("latest_rank", F.rank().over(w9))
    .filter(F.col("latest_rank") == 1)
)

ranked_subscriptions_2 = (
    ranked_subscriptions
    .withColumn("latest_rank_2", F.row_number().over(w10))
    .filter(F.col("latest_rank_2") == 1)
)

result = (
    ranked_subscriptions_2
    .select(
        "id",
        "segment",
        "monthly_fee_share",
        "monthly_fee_avg_2",
        "running_monthly_fee",
        "last_known_credit_limit",
        "latest_rank",
        F.lead("monthly_fee_share", 2).over(w11).alias("next_monthly_fee_share"),
        F.sum("running_monthly_fee").over(w_ordered).alias("cumulative_running_monthly_fee"),
        F.row_number().over(w13).alias("seq_no"),
        F.first("segment", ignorenulls=True).over(w_ordered).alias("first_known_segment"),
        F.last("segment", ignorenulls=True).over(w_grp).alias("last_known_segment_2"),
    )
)
