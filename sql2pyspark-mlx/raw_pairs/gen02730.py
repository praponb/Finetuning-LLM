from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_latest = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"))
)

result = (
    with_latest
    .select(
        "id",
        "account_type",
        "amount",
        F.when(F.col("account_type").rlike("^(LOAN|SAVINGS)$"), "EXTENDED").when(F.col("account_type").rlike("^(CHECKING|CREDIT)$"), "GROUP_B").otherwise("CORE").alias("account_type_zone"),
    )
    .orderBy("amount")
)
