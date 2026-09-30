from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

enriched = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.city"), F.col("c.credit_limit"))
)

enriched_subscriptions = (
    enriched
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.age"))
)

cleaned = (
    enriched_subscriptions
    .filter((F.col("end_date") > F.lit("2024-04-06").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.initcap(F.trim("plan")).alias("plan_title"))
)

result = (
    cleaned
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "credit_limit",
        "age",
        "plan_title",
        F.when(F.col("plan").rlike("^(enterprise|premium)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("plan_zone"),
        F.col("sum_order_discount"),
    )
    .orderBy(F.col("monthly_fee").asc_nulls_last())
)
