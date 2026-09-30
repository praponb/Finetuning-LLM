from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-05-23", "2024-03-01"))
    .select(
        "id",
        "account_type",
        "opened_date",
        F.dense_rank().over(w).alias("opened_date_dense_rank"),
    )
    .orderBy(F.col("opened_date_dense_rank").desc(), "id")
)
