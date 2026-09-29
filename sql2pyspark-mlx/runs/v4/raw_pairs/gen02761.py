from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "customer_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

enriched = (
    df_orders
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w))
    .withColumn("cumulative_discount", F.sum("discount").over(w2))
)

result = (
    enriched
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.when(F.col("channel").rlike("^(web|store)$"), "GROUP_B").otherwise("CORE").alias("channel_zone"),
        F.col("min_account_balance"),
    )
)
