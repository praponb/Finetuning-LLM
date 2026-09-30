from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("started_at", "id")
w_part_2 = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id")
w_part_3 = Window.orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.row_number().over(w_part).alias("row_seq"),
        F.row_number().over(w_part_2).alias("seq_no"),
        F.row_number().over(w_part_3).alias("seq_no_2"),
    )
)
