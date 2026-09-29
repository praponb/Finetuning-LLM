from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("account_type", "status").orderBy("balance", "id")

result = (
    df_accounts
    .filter(F.col("opened_date") > "2023-06-01")
    .select(
        "*",
        F.lead("balance").over(w_recent).alias("next_balance"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)
