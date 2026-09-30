from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w_ordered = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

metrics = (
    df_sessions
    .withColumn("row_seq", F.row_number().over(w_running))
    .withColumn("row_seq_2", F.row_number().over(w_running))
    .withColumn("seq_no", F.row_number().over(w_ordered))
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "row_seq",
        "row_seq_2",
        "seq_no",
    )
)
