from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-08-24").cast("date"))
    .withColumn("start_date_plus_82d", F.date_add("start_date", 82))
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.name"), F.col("d.country"), F.col("d.segment"))
)

valid = (
    with_dims
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "start_date_plus_82d",
        "name",
        "country",
        F.when(F.col("plan").isin(["standard", "premium"]), "Group A").otherwise("Other").alias("plan_group"),
        F.round((F.col("monthly_fee") * 0.9), 2).alias("monthly_fee_scaled"),
    )
)

result = (
    valid
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "start_date_plus_82d",
        "country",
        "plan_group",
        F.when(F.col("plan").rlike("^(premium|enterprise)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("plan_zone"),
    )
    .orderBy(F.col("id").desc(), "start_date_plus_82d")
)
