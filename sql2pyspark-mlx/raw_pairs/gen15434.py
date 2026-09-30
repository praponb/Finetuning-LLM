from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")

ranked = (
    df_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "account_type",
        "opened_date",
        "dedup_rank",
        F.when(F.col("account_type").rlike("^(CHECKING|LOAN)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("account_type_zone"),
    )
    .orderBy(F.col("opened_date").desc())
)
