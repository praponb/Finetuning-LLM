from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-08-29 07:00:00").cast("timestamp")) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.date_format("started_at", "MMM yyyy").alias("started_at_label"),
        F.trim("device").alias("clean_device"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq_2"),
    )
)
