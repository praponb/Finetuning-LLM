from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("account_type").orderBy("balance_int", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("status").eqNullSafe(F.lit("OPEN"))))
    .select("*", F.col("balance").cast("int").alias("balance_int"))
)

result = (
    base
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        F.max("balance").over(w_part).alias("last3_max_balance"),
        F.sum("balance").over(w_part).alias("last3_sum_balance"),
        F.avg("balance").over(w_part).alias("last3_avg_balance"),
    )
)
