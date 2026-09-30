from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id").orderBy("is_premium", "id")
w_recent_2 = Window.partitionBy("customer_id").orderBy("start_date_weekofyear", "id")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("shipping_fee").alias("max_order_shipping_fee"))

cleaned = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-07-18").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        F.weekofyear("start_date").alias("start_date_weekofyear"),
        F.when(F.col("plan") == "premium", 1).otherwise(0).alias("is_premium"),
    )
)

cleaned_subscriptions = (
    cleaned
    .filter(F.col("customer_id").isNull())
    .withColumn("plan_lower", F.lower("plan"))
    .withColumn("plan_title", F.initcap(F.trim("plan")))
)

scored = (
    cleaned_subscriptions
    .filter(F.col("plan").isin(["standard", "premium", "enterprise"]))
    .select(
        "*",
        F.lag("is_premium").over(w_recent).alias("prev_is_premium"),
        F.lag("monthly_fee").over(w_recent).alias("prev_monthly_fee"),
        F.lead("monthly_fee").over(w_recent).alias("next_monthly_fee"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.lag("is_premium", 1).over(w_recent_2).alias("prev_is_premium_2"),
    )
)

base = (
    scored
    .filter(F.col("end_date") > F.lit("2023-09-20").cast("date"))
    .select(
        "*",
        (F.col("next_monthly_fee") + F.col("prev_is_premium_2")).alias("next_monthly_fee_plus_prev_is_premium_2"),
    )
)

result = (
    base
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "start_date_weekofyear",
        "is_premium",
        "prev_is_premium",
        "event_seq",
        F.when(F.col("plan").rlike("^(basic|enterprise)$"), "GROUP_B").otherwise("EXTENDED").alias("plan_zone"),
        F.col("max_order_shipping_fee"),
    )
    .orderBy(F.col("start_date_weekofyear").asc_nulls_last())
)
