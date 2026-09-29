from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "status",
        "opened_date",
        F.lag("opened_date", 1).over(w).alias("prev_opened_date"),
    )
)
