from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id")

valid = (
    df_sessions
    .filter((F.col("started_at") > F.to_date(F.lit("2023-10-08"))) & F.col("country").isNotNull() & F.col("customer_id").isNotNull())
    .withColumn("device_browser", F.concat("device", F.lit(", "), "browser"))
)

result = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)
