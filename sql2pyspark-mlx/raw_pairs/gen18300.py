from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy(F.year("opened_date"), "status").orderBy("balance", "id")
w3 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.avg("balance").over(w).alias("balance_avg_5"),
        F.row_number().over(w2).alias("seq_no"),
        F.last("balance", ignorenulls=True).over(w3).alias("last_known_balance"),
    )
)
