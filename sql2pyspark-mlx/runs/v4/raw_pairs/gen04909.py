from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "status",
        "account_type",
        "opened_date",
        F.lag("opened_date", 1).over(w).alias("prev_opened_date"),
    )
)
