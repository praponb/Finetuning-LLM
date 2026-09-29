from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("opened_date") > "2025-05-01")
    .select(
        "id",
        "account_type",
        "status",
        "opened_date",
        F.row_number().over(w).alias("rn"),
    )
)
