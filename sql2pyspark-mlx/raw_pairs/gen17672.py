from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "opened_date",
        F.lead("opened_date", 2).over(w).alias("next_opened_date"),
    )
    .orderBy("opened_date")
)
