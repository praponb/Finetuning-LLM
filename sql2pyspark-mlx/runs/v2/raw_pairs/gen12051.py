from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.col("opened_date").between("2025-07-24", "2025-08-29"))
    .select(
        "id",
        "status",
        "balance",
        "opened_date",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
    .select("id", "status", "balance", "opened_date")
    .orderBy(F.col("status").desc())
)
