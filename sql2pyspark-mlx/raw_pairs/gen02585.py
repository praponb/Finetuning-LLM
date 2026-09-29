from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("opened_date"), "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("MONTH", "opened_date")).orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

scored = (
    df_accounts
    .filter(F.col("account_type") == "CHECKING")
    .withColumn("cumulative_balance", F.sum("balance").over(w))
    .withColumn("first_known_balance", F.first("balance", ignorenulls=True).over(w2))
    .withColumn("last_known_balance", F.last("balance", ignorenulls=True).over(w3))
)

matched_keys = (
    scored
    .select("status")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
        .select("status")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("id")
    .intersectAll(
        df_accounts
        .filter(F.col("customer_id").isin([10, 23]))
        .select("id")
    )
)

verified_accounts = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified_accounts
    .filter((F.col("opened_date") >= F.lit("2023-09-15").cast("date")) & (~F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .select(
        "id",
        "customer_id",
        "opened_date",
        "cumulative_balance",
        "first_known_balance",
        "last_known_balance",
        F.round((F.col("cumulative_balance") * 1.1), 2).alias("cumulative_balance_adjusted"),
        F.date_trunc("WEEK", "opened_date").alias("opened_date_period"),
    )
)

result = (
    cleaned
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "first_known_balance",
        "cumulative_balance_adjusted",
        F.col("max_order_amount"),
    )
    .orderBy("first_known_balance")
)
