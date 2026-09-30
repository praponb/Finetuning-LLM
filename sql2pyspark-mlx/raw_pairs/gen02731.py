from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id", "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("opened_date"))
w3 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_accounts
    .filter(F.col("account_type") == "SAVINGS")
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.sum("balance").over(w_recent).alias("running_balance"),
        F.sum("balance").over(w2).alias("opened_date_year_sum_balance"),
        F.first("status", ignorenulls=True).over(w3).alias("first_known_status"),
    )
)

result = (
    with_windows
    .select("id", "customer_id", "account_type", "opened_date")
    .orderBy(F.col("id").desc_nulls_first(), F.col("customer_id").desc())
)
