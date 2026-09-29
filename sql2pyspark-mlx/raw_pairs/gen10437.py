from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id")
w_recent = Window.partitionBy(F.date_trunc("WEEK", "opened_date"), "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        F.row_number().over(w).alias("seq_no"),
        F.sum("balance").over(w_recent).alias("running_balance"),
    )
)
