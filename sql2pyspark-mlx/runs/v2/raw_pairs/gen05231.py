from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "status",
        "opened_date",
        "balance",
        F.lead("balance", 1, 0).over(w).alias("next_balance"),
    )
)
