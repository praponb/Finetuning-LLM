from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "opened_date"), "customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy("opened_date", "id")

with_windows = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.row_number().over(w).alias("event_seq"),
        F.lag("balance", 1).over(w2).alias("prev_balance"),
    )
)

result = with_windows.select("id", "balance", "status", "event_seq", "prev_balance")
