from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")

base = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-03-22").cast("date")) & (~F.col("status").eqNullSafe(F.lit("OPEN"))))
    .withColumn("opened_date_minus_85d", F.date_sub("opened_date", 85))
)

result = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "opened_date_minus_85d",
    )
)
