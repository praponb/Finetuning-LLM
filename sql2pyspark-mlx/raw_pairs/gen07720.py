from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("opened_date"), "account_type").orderBy("balance", "id")
w4 = Window.orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        F.lag("balance", 1).over(w).alias("prev_balance"),
        F.first("status", ignorenulls=True).over(w2).alias("first_known_status"),
        F.ntile(3).over(w3).alias("balance_bucket"),
        F.row_number().over(w4).alias("event_seq"),
    )
)
