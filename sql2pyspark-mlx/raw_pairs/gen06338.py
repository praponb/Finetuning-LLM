from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("is_active").desc_nulls_last(), "id")
w2 = Window.partitionBy("plan", "city").orderBy("start_date")

base = (
    df_subscriptions
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-10-06")))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.upper("plan").alias("plan_upper"),
    )
)

base_subscriptions = (
    base
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("status") == "ACTIVE", 1).otherwise(0).alias("is_active"),
        F.date_format("end_date", "MMM yyyy").alias("end_date_label"),
    )
)

ranked = (
    base_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined = (
    ranked
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.credit_limit") >= 7800)
    .select(
        "src.id",
        "src.customer_id",
        "src.plan",
        "src.start_date",
        "src.monthly_fee",
        "src.status",
        "src.end_date_label",
        F.col("d.signup_date"),
        F.col("d.city"),
        F.col("d.age"),
    )
)

deduped = (
    joined
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "plan", "start_date", "end_date_label", "signup_date", "city", "age")
)

result = deduped.select("id", "plan", "start_date", "end_date_label", "city")
