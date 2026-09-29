from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

deduped = (
    df_sessions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.when(F.col("device").rlike("^(mobile|tablet)$"), "PRIORITY").when(F.col("device").rlike("^(desktop)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("device_zone"),
    )
    .orderBy("country")
)
