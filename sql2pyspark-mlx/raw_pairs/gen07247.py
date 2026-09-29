from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("account_type", "customer_id").orderBy("balance", "id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-03-04").cast("date")) & (~F.col("status").eqNullSafe(F.lit("FROZEN"))))
    .withColumn("balance_int", F.col("balance").cast("int"))
    .withColumn("status_clean", F.coalesce("status", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .filter(F.col("status") != "CLOSED")
    .select(
        "id",
        "balance",
        "opened_date",
        "balance_int",
        F.row_number().over(w).alias("row_seq"),
        F.lead("balance", 1).over(w2).alias("next_balance"),
    )
)
