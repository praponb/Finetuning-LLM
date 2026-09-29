from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.partitionBy(F.month("opened_date"), "status").orderBy("balance", "id")
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.col("status") != "CLOSED")
    .select(
        "id",
        "customer_id",
        "balance",
        F.lag("balance", 1).over(w).alias("prev_balance"),
        F.ntile(4).over(w2).alias("balance_bucket"),
        F.sum("balance").over(w3).alias("running_balance"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)
