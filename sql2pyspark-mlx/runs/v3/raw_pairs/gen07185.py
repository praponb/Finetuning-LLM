from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy("opened_date", "id")
w2 = Window.orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("balance").isNotNull())
    .select(
        "id",
        "status",
        "account_type",
        "opened_date",
        "balance",
        F.lead("balance", 2).over(w).alias("next_balance"),
        F.row_number().over(w2).alias("opened_date_row"),
    )
)
