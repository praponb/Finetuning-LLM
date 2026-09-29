from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "opened_date",
        "balance",
        F.lag("balance", 1).over(w).alias("prev_balance"),
    )
)
