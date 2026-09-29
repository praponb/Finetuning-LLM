from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .select(
        "id",
        "status",
        "balance",
        "opened_date",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 2)
)
