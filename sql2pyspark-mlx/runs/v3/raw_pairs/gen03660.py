from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 2)
    .select("id", "account_type", "balance", "opened_date")
)
