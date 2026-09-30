from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "status",
        "balance",
        "opened_date",
        F.first("opened_date").over(w).alias("first_opened_date"),
    )
    .orderBy("status")
)
