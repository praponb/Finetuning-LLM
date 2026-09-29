from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id").orderBy(F.col("shipping_fee").asc_nulls_first(), "id")
w2 = Window.partitionBy("customer_id", "store_id").orderBy(F.col("amount").desc_nulls_last(), "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

filtered = (
    df_orders
    .filter(F.col("channel") == "store")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        (F.col("shipping_fee") * F.col("amount")).alias("amount_with_shipping"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "amount_with_shipping",
    )
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.unit_price").between(280, 320)), "left_semi")
)

deduped = (
    not_flagged
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "shipping_fee",
        "amount_with_shipping",
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("amount_with_shipping").asc_nulls_last())
)
