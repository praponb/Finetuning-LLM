from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee_scaled").desc_nulls_first(), "id")
w2 = Window.partitionBy("plan").orderBy(F.col("is_active").desc(), "id")
w3 = Window.orderBy("monthly_fee", "id")
w4 = Window.partitionBy("plan").orderBy("latest_rank", "id")
w5 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_scaled").desc(), "id")
w6 = Window.partitionBy("plan").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("plan").orderBy(F.col("last_known_monthly_fee").desc(), "id")

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "monthly_fee",
        F.when(F.col("status") == "ACTIVE", 1).otherwise(0).alias("is_active"),
        F.round((F.col("monthly_fee") * 1.07), 2).alias("monthly_fee_scaled"),
    )
)

filtered = (
    base
    .filter(F.col("monthly_fee") < 240)
    .withColumn("is_active_rounded", F.round("is_active", 1))
    .withColumn("plan_group", F.when(F.col("plan").isin(["basic", "enterprise"]), "Group A").otherwise("Other"))
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

base_subscriptions = (
    ranked
    .filter(F.col("monthly_fee_scaled") >= 25)
    .select(
        "*",
        F.when(F.col("monthly_fee") >= 255, "High").when(F.col("monthly_fee") >= 116, "Medium").otherwise("Low").alias("monthly_fee_band"),
    )
)

with_windows = (
    base_subscriptions
    .filter(F.col("monthly_fee").between(140, 200))
    .select(
        "id",
        "plan",
        "monthly_fee",
        "is_active",
        "plan_group",
        "latest_rank",
        "monthly_fee_band",
        F.ntile(10).over(w2).alias("is_active_bucket"),
        F.ntile(4).over(w3).alias("monthly_fee_bucket"),
        F.row_number().over(w4).alias("event_seq"),
        F.ntile(4).over(w5).alias("monthly_fee_scaled_bucket"),
        F.last("monthly_fee", ignorenulls=True).over(w6).alias("last_known_monthly_fee"),
    )
)

ranked_subscriptions = (
    with_windows
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "plan",
        "is_active",
        "latest_rank",
        "monthly_fee_band",
        "is_active_bucket",
        "monthly_fee_bucket",
        "event_seq",
        "monthly_fee_scaled_bucket",
    )
)

result = (
    ranked_subscriptions
    .select(
        "id",
        "plan",
        "is_active",
        "latest_rank",
        "monthly_fee_band",
        "is_active_bucket",
        "monthly_fee_bucket",
        "monthly_fee_scaled_bucket",
        F.when(F.col("plan").rlike("^(basic|premium)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("plan_zone"),
    )
)
