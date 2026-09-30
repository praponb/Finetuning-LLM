from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("start_date", "id")
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("amount").alias("avg_order_amount"))

valid = (
    df_subscriptions
    .filter((F.col("start_date") > F.to_date(F.lit("2023-04-10"))) & (F.col("plan") == "enterprise"))
    .withColumn("start_date_month_end", F.last_day("start_date"))
)

joined = (
    valid
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.start_date",
        "s.status",
        "s.start_date_month_end",
        F.col("d.email"),
    )
)

latest = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

remaining_keys = (
    latest
    .select("id", "status")
    .intersect(
        df_subscriptions
        .filter(F.col("status").isin(["PAUSED", "CANCELLED", "EXPIRED"]))
        .select("id", "status")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "start_date_month_end",
        "email",
        F.col("avg_order_amount"),
    )
    .orderBy(F.col("customer_id").desc_nulls_last())
)
