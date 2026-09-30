from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")
w2 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

with_windows = (
    df_accounts
    .withColumn("next_balance", F.lead("balance").over(w))
    .withColumn("prev_balance", F.lag("balance").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("first_known_balance", F.first("balance", ignorenulls=True).over(w2))
)

result = (
    with_windows
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "next_balance",
        "prev_balance",
        "row_seq",
        F.col("sum_order_discount"),
    )
)
