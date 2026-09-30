from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "started_at",
        "device",
        F.row_number().over(w).alias("row_num"),
        F.ntile(2).over(w2).alias("quartile"),
    )
)
