from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("amount").alias("sum_order_amount"))

joined = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.segment"), F.col("c.email"), F.col("c.age"))
)

latest = (
    joined
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

remaining_keys = (
    latest
    .select("plan", "id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("plan") != "premium")
        .select("plan", "id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "segment",
        "email",
        "age",
        F.col("sum_order_amount"),
    )
    .orderBy("end_date", "sum_order_amount")
)
