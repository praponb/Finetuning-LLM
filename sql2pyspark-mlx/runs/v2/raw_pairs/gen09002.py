from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.col("opened_date").between("2025-04-01", "2025-07-01"))
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        F.first("status").over(w).alias("first_status"),
    )
    .orderBy("status", F.col("first_status").desc())
)
