from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.month("opened_date").isin([4, 8, 9]))
    .select(
        "id",
        "status",
        "opened_date",
        "balance",
        (F.col("balance") - F.lag("balance", 2).over(w)).alias("balance_change"),
    )
)
