from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "opened_date",
        "balance",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
)
