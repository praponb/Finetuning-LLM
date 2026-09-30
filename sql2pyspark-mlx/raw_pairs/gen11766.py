from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")

ranked = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.when(F.col("country").rlike("^(JP|TH)$"), "OTHER_ZONE").when(F.col("country").rlike("^(US|SG)$"), "PRIORITY").otherwise("GROUP_A").alias("country_zone"),
    )
    .orderBy(F.col("device").desc())
)
