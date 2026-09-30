from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy("discount", "id")
w3 = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-05-05"))) & F.col("store_id").isNotNull() & F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "amount",
        "discount",
        "shipping_fee",
        F.date_add("order_date", 87).alias("order_date_plus_87d"),
        F.initcap(F.trim("status")).alias("status_title"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.paid_at").alias("payment_paid_at"))
)

joined = (
    with_last
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.credit_limit"), F.col("c.email"))
)

metrics = (
    joined
    .withColumn("next_amount", F.lead("amount", 1).over(w2))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w3))
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "discount",
        "shipping_fee",
        "order_date_plus_87d",
        "status_title",
        "channel_filled",
    )
    .orderBy(F.col("customer_id").desc_nulls_first())
)
