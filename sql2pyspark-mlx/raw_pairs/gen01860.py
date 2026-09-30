from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("amount").desc_nulls_last())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("amount").alias("min_order_amount"))

filtered = (
    df_orders
    .filter(F.month("order_date").isin([1, 2, 4]))
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.length("status").alias("status_length"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.quantity") <= 4), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    ranked
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(~F.col("c.country").isin(["UK", "DE", "SG"]))
    .select(
        "s.id",
        "s.customer_id",
        "s.channel",
        "s.status_length",
        F.col("c.country"),
        F.col("c.signup_date"),
        F.col("c.email"),
    )
)

result = (
    enriched
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "signup_date", "email", F.col("min_order_amount"))
    .orderBy(F.col("id").asc_nulls_first())
)
