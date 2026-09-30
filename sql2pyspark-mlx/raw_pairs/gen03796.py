from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")

deduped = (
    df_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "rn",
        F.when(F.col("status").rlike("^(CANCELLED|PAUSED)$"), "GROUP_A").when(F.col("status").rlike("^(ACTIVE|EXPIRED)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("status_zone").asc_nulls_last(), "end_date")
)
