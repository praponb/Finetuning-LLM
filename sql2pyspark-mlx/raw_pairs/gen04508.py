from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("started_at_date").desc(), "id")
w_part = Window.partitionBy("device", "customer_id").orderBy("started_at_date", "id")
w_grp = Window.partitionBy("device").orderBy("started_at_date", "id")

valid = (
    df_sessions
    .filter(~F.col("browser").isin(["Chrome", "Safari"]))
    .select(
        "id",
        "customer_id",
        "device",
        F.to_date("started_at").alias("started_at_date"),
    )
)

result = (
    valid
    .select(
        "id",
        "customer_id",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
        F.row_number().over(w_part).alias("seq_no"),
        F.row_number().over(w_grp).alias("event_seq_3"),
    )
)
