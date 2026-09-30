from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy(F.month("end_date"), "plan")
w4 = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("start_date", "id")
w6 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

base = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-03-18").cast("date")) & (F.col("status") == "CANCELLED"))
    .select("*", F.weekofyear("end_date").alias("end_date_weekofyear"))
)

scored = (
    base
    .withColumn("monthly_fee_avg_3_prior", F.avg("monthly_fee").over(w))
    .withColumn("monthly_fee_avg_5", F.avg("monthly_fee").over(w2))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4))
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
)

scored_subscriptions = (
    scored
    .select(
        "*",
        F.min("monthly_fee_avg_3_prior").over(w6).alias("running_min_monthly_fee_avg_3_prior"),
        F.avg("monthly_fee_avg_3_prior").over(w6).alias("running_avg_monthly_fee_avg_3_prior"),
        F.sum("monthly_fee_avg_3_prior").over(w6).alias("running_sum_monthly_fee_avg_3_prior"),
    )
)

valid = (
    scored_subscriptions
    .filter((F.col("start_date") >= F.to_date(F.lit("2023-08-15"))) & (F.col("plan") == "basic"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "monthly_fee_avg_3_prior",
        "monthly_fee_share",
        "last_known_monthly_fee",
        "row_seq",
        "running_min_monthly_fee_avg_3_prior",
        "running_sum_monthly_fee_avg_3_prior",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.country").isin(["SG", "DE", "TH"]))
    .select(
        "e.id",
        "e.customer_id",
        "e.plan",
        "e.start_date",
        "e.end_date",
        "e.monthly_fee_avg_3_prior",
        "e.monthly_fee_share",
        "e.row_seq",
        "e.monthly_fee_int",
        F.col("c.age"),
        F.col("c.city"),
    )
)

kept_keys = (
    with_dims
    .select("customer_id", "plan")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee") >= 200)
        .select("customer_id", "plan")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "row_seq",
        "monthly_fee_int",
        "city",
        F.when(F.col("city").rlike("^(Berlin|Tokyo)$"), "GROUP_A").otherwise("CORE").alias("city_zone"),
        F.col("avg_order_discount"),
    )
)
